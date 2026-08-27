package oversqlite_e2e

import (
	"crypto/sha256"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversync"
)

func retryableBundleWriteOptionsForTest() oversync.RetryableBundleWriteOptions {
	seed := uuid.New()
	return oversync.RetryableBundleWriteOptions{
		OperationHash:       sha256.Sum256(seed[:]),
		OperationValidUntil: time.Now().UTC().Truncate(time.Microsecond).Add(24 * time.Hour),
	}
}
