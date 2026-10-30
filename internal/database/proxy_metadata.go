package database

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
)

type proxyMetadataUpdate struct {
	ID            uint64
	Country       string
	EstimatedType string
}

type proxyMetadataProvider uint8

const (
	localProxyMetadata proxyMetadataProvider = iota
	ipGeolocationProxyMetadata
)

// Local enrichment must preserve fresh provider results. Provider projections
// use the same writer inside their cursor transaction and take precedence.
func updateProxyMetadataBatch(tx *gorm.DB, batch []proxyMetadataUpdate, provider proxyMetadataProvider) error {
	if len(batch) == 0 {
		return nil
	}

	values := make([]string, len(batch))
	args := make([]any, 0, len(batch)*3)
	for i, proxy := range batch {
		values[i] = "(?::bigint, ?::text, ?::text)"
		args = append(args, proxy.ID, proxy.Country, proxy.EstimatedType)
	}

	query := fmt.Sprintf(`UPDATE proxies AS p
SET country = tmp.country,
    estimated_type = tmp.estimated_type
FROM (VALUES %s) AS tmp(id, country, estimated_type)
WHERE p.id = tmp.id
  AND (p.country, p.estimated_type) IS DISTINCT FROM (tmp.country, tmp.estimated_type)`, strings.Join(values, ","))
	switch provider {
	case localProxyMetadata:
		guard, guardArgs := ipGeolocationMetadataGuard("tmp")
		query += guard
		args = append(args, guardArgs...)
	case ipGeolocationProxyMetadata:
	default:
		return fmt.Errorf("unknown proxy metadata provider %d", provider)
	}
	return tx.Exec(query, args...).Error
}
