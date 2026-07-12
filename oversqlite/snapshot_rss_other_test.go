//go:build !darwin && !linux

package oversqlite

func currentProcessPeakRSSBytes() uint64 {
	return 0
}
