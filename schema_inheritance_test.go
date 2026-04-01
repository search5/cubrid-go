package cubrid

import "testing"

func TestSchemaTypeSuperClassConstant(t *testing.T) {
	if SchemaTypeSuperClass != 3 {
		t.Fatalf("SchemaTypeSuperClass: got %d, want 3", SchemaTypeSuperClass)
	}
}

func TestInheritanceInfoStructure(t *testing.T) {
	info := &InheritanceInfo{
		ClassName:    "child_table",
		SuperClasses: []string{"parent_table"},
		SubClasses:   []string{"grandchild_table"},
	}

	if info.ClassName != "child_table" {
		t.Fatalf("ClassName: got %q, want %q", info.ClassName, "child_table")
	}
	if len(info.SuperClasses) != 1 || info.SuperClasses[0] != "parent_table" {
		t.Fatalf("SuperClasses: got %v, want [parent_table]", info.SuperClasses)
	}
	if len(info.SubClasses) != 1 || info.SubClasses[0] != "grandchild_table" {
		t.Fatalf("SubClasses: got %v, want [grandchild_table]", info.SubClasses)
	}
}
