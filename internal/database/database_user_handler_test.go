package database

import (
	"testing"

	"magpie/internal/api/dto"
	"magpie/internal/domain"
)

func TestLegacyDefaultChangeAdvancesFullRefreshRevision(t *testing.T) {
	db := setupProxyTagTestDB(t)
	workspace := domain.Workspace{Name: "Scalar defaults", HTTPProtocol: true, Timeout: 1000, TransportProtocol: "tcp"}
	if err := db.Create(&workspace).Error; err != nil {
		t.Fatal(err)
	}
	for _, wantRevision := range []uint64{1, 2} {
		if err := UpdateWorkspaceSettings(workspace.ID, 0, dto.UserSettings{ProvidedFields: map[string]bool{"timeout": true}, Timeout: 2000}); err != nil {
			t.Fatal(err)
		}
		var saved domain.Workspace
		if err := db.First(&saved, workspace.ID).Error; err != nil {
			t.Fatal(err)
		}
		if saved.CheckerRevision != wantRevision || saved.CheckerFullRevision != 1 || saved.Timeout != 2000 || saved.CheckerConfig != nil {
			t.Fatal("legacy settings lost their full-refresh watermark or changed profile format", saved)
		}
	}
}
