package protocol

import (
	"bytes"
	"testing"
)

func TestAutoCommit(t *testing.T) {
	ci := NewCASInfo()
	if ci.AutoCommit() {
		t.Fatal("default should be false")
	}
	ci.SetAutoCommit(true)
	if !ci.AutoCommit() {
		t.Fatal("should be true")
	}
	ci.SetAutoCommit(false)
	if ci.AutoCommit() {
		t.Fatal("should be false again")
	}
}

func TestWriteFloat(t *testing.T) {
	var buf bytes.Buffer
	WriteFloat(&buf, 3.14)
	if buf.Len() != 4 {
		t.Fatalf("len: %d", buf.Len())
	}
}

func TestWriteDouble(t *testing.T) {
	var buf bytes.Buffer
	WriteDouble(&buf, 2.718)
	if buf.Len() != 8 {
		t.Fatalf("len: %d", buf.Len())
	}
}

func TestWriteByteFunc(t *testing.T) {
	var buf bytes.Buffer
	WriteByte(&buf, 0x42)
	if buf.Len() != 1 || buf.Bytes()[0] != 0x42 {
		t.Fatal("mismatch")
	}
}

func TestWriteBytes(t *testing.T) {
	var buf bytes.Buffer
	WriteBytes(&buf, []byte{0x01, 0x02, 0x03})
	if buf.Len() != 7 { // 4 (len) + 3 (data)
		t.Fatalf("len: %d", buf.Len())
	}
}

func TestReadBytes(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03}
	r := bytes.NewReader(data)
	result, err := ReadBytes(r, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(result, data) {
		t.Fatalf("mismatch: %v", result)
	}
}

func TestCubridTypeString(t *testing.T) {
	types := []CubridType{
		CubridTypeNull, CubridTypeChar, CubridTypeString, CubridTypeNChar,
		CubridTypeVarNChar, CubridTypeBit, CubridTypeVarBit, CubridTypeNumeric,
		CubridTypeInt, CubridTypeShort, CubridTypeMonetary, CubridTypeFloat,
		CubridTypeDouble, CubridTypeDate, CubridTypeTime, CubridTypeTimestamp,
		CubridTypeSet, CubridTypeMultiSet, CubridTypeSequence, CubridTypeObject,
		CubridTypeResultSet, CubridTypeBigInt, CubridTypeDatetime,
		CubridTypeBlob, CubridTypeClob, CubridTypeEnum,
		CubridTypeUShort, CubridTypeUInt, CubridTypeUBigInt,
		CubridTypeTsTz, CubridTypeTsLtz, CubridTypeDtTz, CubridTypeDtLtz,
		CubridTypeJSON, CubridType(99),
	}
	for _, ct := range types {
		s := ct.String()
		if s == "" {
			t.Fatalf("empty string for type %d", ct)
		}
	}
}

func TestStmtTypeIsQuery(t *testing.T) {
	queries := []StmtType{StmtSelect, StmtCall, StmtCallSp, StmtEvaluate,
		StmtSelectUpdate, StmtGetStats, StmtGetIsoLvl, StmtGetTimeout,
		StmtGetOptLvl, StmtGetTrigger, StmtGetLdb}
	for _, st := range queries {
		if !st.IsQuery() {
			t.Fatalf("%d should be query", st)
		}
	}

	nonQueries := []StmtType{StmtInsert, StmtUpdate, StmtDelete, StmtCreateTable,
		StmtDropTable, StmtAlterTable, StmtMerge}
	for _, st := range nonQueries {
		if st.IsQuery() {
			t.Fatalf("%d should not be query", st)
		}
	}
}

func TestReadNullTermStringEmpty(t *testing.T) {
	// length = 0
	var buf bytes.Buffer
	WriteInt(&buf, 0)
	r := bytes.NewReader(buf.Bytes())
	s, err := ReadNullTermString(r)
	if err != nil {
		t.Fatal(err)
	}
	if s != "" {
		t.Fatalf("got %q, want empty", s)
	}
}
