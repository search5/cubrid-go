package cubrid

import (
	"testing"
)

func TestSchemaTypeConstants(t *testing.T) {
	tests := []struct {
		st   SchemaType
		want int32
	}{
		{SchemaClass, 1},
		{SchemaVClass, 2},
		{SchemaAttribute, 4},
		{SchemaConstraint, 11},
		{SchemaPrimaryKey, 16},
		{SchemaImportedKeys, 17},
		{SchemaExportedKeys, 18},
	}
	for _, tt := range tests {
		if int32(tt.st) != tt.want {
			t.Errorf("%v = %d, want %d", tt.st, tt.st, tt.want)
		}
	}
}
