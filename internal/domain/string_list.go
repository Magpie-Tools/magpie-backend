package domain

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

// StringList stores a slice of strings inside a JSON column.
type StringList []string

// Value implements driver.Valuer so StringList can be stored as JSON.
func (s StringList) Value() (driver.Value, error) {
	if len(s) == 0 {
		return []byte("[]"), nil
	}

	data, err := json.Marshal([]string(s))
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Scan implements sql.Scanner to hydrate the StringList from the database.
func (s *StringList) Scan(value any) error {
	if value == nil {
		*s = nil
		return nil
	}

	switch v := value.(type) {
	case []byte:
		return s.unmarshal(v)
	case string:
		return s.unmarshal([]byte(v))
	default:
		return fmt.Errorf("domain.StringList: unsupported type %T", value)
	}
}

func (s *StringList) unmarshal(data []byte) error {
	if len(data) == 0 {
		*s = nil
		return nil
	}

	var parsed []string
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}
	*s = parsed
	return nil
}

// Clone returns a copy of the underlying slice to avoid sharing memory.
func (s StringList) Clone() []string {
	if len(s) == 0 {
		return nil
	}
	out := make([]string, len(s))
	copy(out, s)
	return out
}
