package sign

import (
	"encoding/base64"
	"errors"
	"testing"
	"time"
)

var (
	testSecret = []byte("test-secret-0123456789")
	testNow    = time.Unix(1_760_000_000, 0)
	testBody   = []byte(`{"id":"evt_1","type":"order.paid","data":{"order_id":"ord_1","amount_cents":500}}`)
)

func TestVerifyValid(t *testing.T) {
	hdr := SignatureHeader(testSecret, "evt_1", testNow, testBody)
	if err := Verify(testSecret, "evt_1", "1760000000", hdr, testBody, testNow, 0); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
}

func TestVerifyTamperedBody(t *testing.T) {
	hdr := SignatureHeader(testSecret, "evt_1", testNow, testBody)
	tampered := []byte(`{"id":"evt_1","type":"order.paid","data":{"order_id":"ord_1","amount_cents":99999}}`)
	if err := Verify(testSecret, "evt_1", "1760000000", hdr, tampered, testNow, 0); !errors.Is(err, ErrNoMatchingSig) {
		t.Fatalf("tampered body: got %v, want ErrNoMatchingSig", err)
	}
}

func TestVerifyWrongSecret(t *testing.T) {
	hdr := SignatureHeader([]byte("attacker-secret"), "evt_1", testNow, testBody)
	if err := Verify(testSecret, "evt_1", "1760000000", hdr, testBody, testNow, 0); !errors.Is(err, ErrNoMatchingSig) {
		t.Fatalf("wrong secret: got %v, want ErrNoMatchingSig", err)
	}
}

func TestVerifyStaleAndFuture(t *testing.T) {
	old := testNow.Add(-time.Hour)
	hdr := SignatureHeader(testSecret, "evt_1", old, testBody)
	err := Verify(testSecret, "evt_1", "1759996400", hdr, testBody, testNow, 0)
	if !errors.Is(err, ErrTimestampTooOld) {
		t.Fatalf("stale: got %v, want ErrTimestampTooOld", err)
	}
	future := testNow.Add(time.Hour)
	hdr = SignatureHeader(testSecret, "evt_1", future, testBody)
	err = Verify(testSecret, "evt_1", "1760003600", hdr, testBody, testNow, 0)
	if !errors.Is(err, ErrTimestampTooNew) {
		t.Fatalf("future: got %v, want ErrTimestampTooNew", err)
	}
}

func TestVerifyRotationMultipleSignatures(t *testing.T) {
	// Header carries an old-secret signature plus the current one; the
	// current one must match (Standard Webhooks rotation pattern).
	oldHdr := SignatureHeader([]byte("old-secret"), "evt_1", testNow, testBody)
	newHdr := SignatureHeader(testSecret, "evt_1", testNow, testBody)
	combined := oldHdr + " " + newHdr
	if err := Verify(testSecret, "evt_1", "1760000000", combined, testBody, testNow, 0); err != nil {
		t.Fatalf("rotation header rejected: %v", err)
	}
}

func TestVerifyMissingAndMalformed(t *testing.T) {
	if err := Verify(testSecret, "", "1760000000", "v1,AAAA", testBody, testNow, 0); !errors.Is(err, ErrMissingHeaders) {
		t.Fatalf("missing id: got %v", err)
	}
	hdr := SignatureHeader(testSecret, "evt_1", testNow, testBody)
	if err := Verify(testSecret, "evt_1", "not-a-number", hdr, testBody, testNow, 0); !errors.Is(err, ErrMalformedHeader) {
		t.Fatalf("malformed ts: got %v", err)
	}
	if err := Verify(testSecret, "evt_1", "1760000000", "v0,AAAA", testBody, testNow, 0); !errors.Is(err, ErrMalformedSigList) {
		t.Fatalf("no v1 sig: got %v", err)
	}
}

func TestWhsecSecretForm(t *testing.T) {
	raw := []byte("base64-form-secret")
	encoded := "whsec_" + base64.StdEncoding.EncodeToString(raw)
	if got := SecretBytes(encoded); string(got) != string(raw) {
		t.Fatalf("whsec_ decode: got %q want %q", got, raw)
	}
	// A signature made with the raw bytes verifies against the whsec_ form.
	hdr := SignatureHeader(raw, "evt_1", testNow, testBody)
	if err := Verify(SecretBytes(encoded), "evt_1", "1760000000", hdr, testBody, testNow, 0); err != nil {
		t.Fatalf("whsec_ verify failed: %v", err)
	}
}

func TestReceiptHMACRoundTrip(t *testing.T) {
	sig := HMACBase64(testSecret, "evt_1|abc123|7|deadbeef|2026-01-01T00:00:00Z")
	if !EqualHMAC(sig, sig) {
		t.Fatal("identical HMACs must compare equal")
	}
	other := HMACBase64(testSecret, "evt_1|abc124|7|deadbeef|2026-01-01T00:00:00Z")
	if EqualHMAC(sig, other) {
		t.Fatal("different payloads must not compare equal")
	}
	if EqualHMAC(sig, "!!!not-base64!!!") {
		t.Fatal("malformed HMAC must not compare equal")
	}
}
