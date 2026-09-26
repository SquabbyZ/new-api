package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestImplicitMasterMigrationWarning pins the three NODE_TYPE cases the request
// names: an unset NODE_TYPE must warn, while an explicit master and an explicit
// slave must stay quiet. The check mirrors common.IsMasterNode, which reads
// os.Getenv, so an explicitly empty NODE_TYPE is the same input as an unset one.
//
// The warning must reach the error-level writer rather than the info-level one,
// because that is the only thing that distinguishes it from the ordinary
// "database migration started" line next to it.
func TestImplicitMasterMigrationWarning(t *testing.T) {
	cases := []struct {
		name     string
		nodeType string
		warns    bool
	}{
		{name: "unset NODE_TYPE warns", nodeType: "", warns: true},
		{name: "explicit master stays quiet", nodeType: "master", warns: false},
		{name: "explicit slave stays quiet", nodeType: "slave", warns: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("NODE_TYPE", tc.nodeType)
			errorLog := captureSysError(t)

			warnImplicitMasterMigration()

			if !tc.warns {
				assert.Empty(t, errorLog.String())
				return
			}
			assert.Contains(t, errorLog.String(), "NODE_TYPE")
			assert.Contains(t, errorLog.String(), "treated as master")
			assert.Contains(t, errorLog.String(), "database migrations")
		})
	}
}
