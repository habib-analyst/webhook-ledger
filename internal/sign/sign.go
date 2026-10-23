// Package sign implements Standard Webhooks-style request signing and
// verification: HMAC-SHA256 over "{msgID}.{timestamp}.{body}", delivered in
// the webhook-signature header as one or more space-separated "v1,<base64>"
// entries (multiple entries support secret rotation).
package sign

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// DefaultTolerance is the maximum accepted clock difference between the
// webhook-timestamp header and the receiver clock, per the Standard
// Webhooks recommendation of five minutes.
const DefaultTolerance = 5 * time.Minute

var (
	ErrMissingHeaders   = errors.New("missing webhook-id, webhook-timestamp or webhook-signature header")
	ErrMalformedHeader  = errors.New("malformed webhook-timestamp header")
	ErrTimestampTooOld  = errors.New("webhook timestamp is too old")
	ErrTimestampTooNew  = errors.New("webhook timestamp is too far in the future")
	ErrNoMatchingSig    = errors.New("no matching v1 signature")
	ErrMalformedSigList = errors.New("malformed webhook-signature header")
)

// SecretBytes normalises a signing secret. Secrets issued in the
// Standard Webhooks format look like "whsec_<base64>"; anything else is
// used as raw bytes.
func SecretBytes(secret string) []byte {
	if strings.HasPrefix(secret, "whsec_") {
		if raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, "whsec_")); err == nil {
			return raw
		}
	}
	return []byte(secret)
}

// Sign returns the base64 HMAC-SHA256 signature for one delivery.
func Sign(secret []byte, msgID string, ts time.Time, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	fmt.Fprintf(mac, "%s.%d.", msgID, ts.Unix())
	mac.Write(body)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// SignatureHeader builds a complete webhook-signature header value.
func SignatureHeader(secret []byte, msgID string, ts time.Time, body []byte) string {
	return "v1," + Sign(secret, msgID, ts, body)
}

// Verify checks a delivery against the shared secret. now is injected so
// tests and the storm harness control the clock. tolerance <= 0 selects
// DefaultTolerance.
func Verify(secret []byte, msgID, tsHeader, sigHeader string, body []byte, now time.Time, tolerance time.Duration) error {
	if msgID == "" || tsHeader == "" || sigHeader == "" {
		return ErrMissingHeaders
	}
	tsUnix, err := strconv.ParseInt(tsHeader, 10, 64)
	if err != nil {
		return ErrMalformedHeader
	}
	if tolerance <= 0 {
		tolerance = DefaultTolerance
	}
	ts := time.Unix(tsUnix, 0)
	if now.Sub(ts) > tolerance {
		return ErrTimestampTooOld
	}
	if ts.Sub(now) > tolerance {
		return ErrTimestampTooNew
	}
	expected := Sign(secret, msgID, ts, body)
	matched := false
	sawV1 := false
	for _, part := range strings.Fields(sigHeader) {
		ver, sig, ok := strings.Cut(part, ",")
		if !ok || ver != "v1" {
			continue
		}
		sawV1 = true
		got, err := base64.StdEncoding.DecodeString(sig)
		if err != nil {
			continue
		}
		want, err := base64.StdEncoding.DecodeString(expected)
		if err != nil {
			continue
		}
		if subtle.ConstantTimeCompare(got, want) == 1 {
			matched = true
		}
	}
	if !sawV1 {
		return ErrMalformedSigList
	}
	if !matched {
		return ErrNoMatchingSig
	}
	return nil
}

// HMACBase64 is the raw primitive used for receipts: base64 HMAC-SHA256 of
// an arbitrary canonical string under the shared secret.
func HMACBase64(secret []byte, canonical string) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(canonical))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// EqualHMAC compares two base64 HMAC values in constant time.
func EqualHMAC(a, b string) bool {
	ab, err1 := base64.StdEncoding.DecodeString(a)
	bb, err2 := base64.StdEncoding.DecodeString(b)
	if err1 != nil || err2 != nil {
		return false
	}
	return subtle.ConstantTimeCompare(ab, bb) == 1
}
