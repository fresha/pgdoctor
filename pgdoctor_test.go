package pgdoctor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		filters       []string
		expectedValid []string
		expectedInval []string
	}{
		{
			name:          "valid check ID",
			filters:       []string{"pg-version"},
			expectedValid: []string{"pg-version"},
			expectedInval: nil,
		},
		{
			name:          "valid category",
			filters:       []string{"configs"},
			expectedValid: []string{"configs"},
			expectedInval: nil,
		},
		{
			name:          "subcheck ID extracts check ID",
			filters:       []string{"connection-efficiency/sessions-fatal"},
			expectedValid: []string{"connection-efficiency"},
			expectedInval: nil,
		},
		{
			name:          "invalid filter",
			filters:       []string{"nonexistent-check"},
			expectedValid: nil,
			expectedInval: []string{"nonexistent-check"},
		},
		{
			name:          "mixed valid and invalid",
			filters:       []string{"pg-version", "invalid-check", "connection-efficiency/subcheck"},
			expectedValid: []string{"pg-version", "connection-efficiency"},
			expectedInval: []string{"invalid-check"},
		},
		{
			name:          "duplicate filters after normalization",
			filters:       []string{"connection-efficiency", "connection-efficiency/sessions-fatal"},
			expectedValid: []string{"connection-efficiency"},
			expectedInval: nil,
		},
		{
			name:          "multiple subchecks same check",
			filters:       []string{"connection-efficiency/sessions-fatal", "connection-efficiency/sessions-idle"},
			expectedValid: []string{"connection-efficiency"},
			expectedInval: nil,
		},
		{
			name:          "category and check from same category",
			filters:       []string{"configs", "pg-version"},
			expectedValid: []string{"configs", "pg-version"},
			expectedInval: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			valid, invalid := ValidateFilters(AllChecks(), tt.filters)

			assert.ElementsMatch(t, tt.expectedValid, valid, "valid filters should match")
			assert.ElementsMatch(t, tt.expectedInval, invalid, "invalid filters should match")
		})
	}
}
