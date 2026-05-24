package bigquery

import "time"

func CheckTableConsistency(lastModifiedTime, snapshotTime time.Time) bool {
	return lastModifiedTime.After(snapshotTime)
}
