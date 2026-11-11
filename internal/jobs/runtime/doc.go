// Package runtime holds the background routines that are not part of the
// checker or scraper loops: statistics admission and ingestion, retention,
// history and snapshot rollups, intelligence refreshes and instance
// heartbeats. Feature packages such as alerts and mail own their routines.
package runtime
