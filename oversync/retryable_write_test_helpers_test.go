package oversync

import (
	"crypto/sha256"
	"time"

	"github.com/google/uuid"
)

func retryableWriteOptionsForTest() RetryableWriteOptions {
	operationID := uuid.New()
	return RetryableWriteOptions{
		OperationID:         operationID,
		OperationHash:       sha256.Sum256(operationID[:]),
		OperationValidUntil: time.Now().UTC().Truncate(time.Microsecond).Add(24 * time.Hour),
	}
}

func retryableBundleWriteOptionsForTest() RetryableBundleWriteOptions {
	seed := uuid.New()
	return RetryableBundleWriteOptions{
		OperationHash:       sha256.Sum256(seed[:]),
		OperationValidUntil: time.Now().UTC().Truncate(time.Microsecond).Add(24 * time.Hour),
	}
}
