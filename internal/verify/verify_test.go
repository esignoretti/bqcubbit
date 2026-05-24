package verify

import "testing"

func TestVerifier_SkipCI(t *testing.T) {
	t.Skip("requires GCP credentials")
}
