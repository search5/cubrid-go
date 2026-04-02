package cubrid

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/search5/cubrid-go/protocol"
)

func TestCubridJsonNew(t *testing.T) {
	j := NewCubridJson(`{"key":"value"}`)
	if j.String() != `{"key":"value"}` {
		t.Fatalf("got %q", j.String())
	}
}

func TestCubridJsonUnmarshal(t *testing.T) {
	j := NewCubridJson(`{"name":"test","count":42}`)
	var result map[string]interface{}
	if err := j.Unmarshal(&result); err != nil {
		t.Fatal(err)
	}
	if result["name"] != "test" {
		t.Fatalf("name: got %v", result["name"])
	}
}

func TestMarshalCubridJson(t *testing.T) {
	data := map[string]int{"a": 1, "b": 2}
	j, err := MarshalCubridJson(data)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]int
	if err := json.Unmarshal([]byte(j.String()), &result); err != nil {
		t.Fatal(err)
	}
	if result["a"] != 1 || result["b"] != 2 {
		t.Fatalf("unexpected result: %v", result)
	}
}

func TestCubridJsonScan(t *testing.T) {
	tests := []struct {
		name string
		src  interface{}
		want string
	}{
		{"string", `[1,2,3]`, `[1,2,3]`},
		{"bytes", []byte(`{"x":1}`), `{"x":1}`},
		{"raw_message", json.RawMessage(`true`), `true`},
		{"nil", nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var j CubridJson
			if err := j.Scan(tt.src); err != nil {
				t.Fatal(err)
			}
			if j.String() != tt.want {
				t.Fatalf("got %q, want %q", j.String(), tt.want)
			}
		})
	}
}

func TestDecodeValueJson(t *testing.T) {
	data := []byte(`{"key":"value"}` + "\x00")
	val, err := decodeValue(protocol.CubridTypeJSON, data)
	if err != nil {
		t.Fatal(err)
	}
	j, ok := val.(*CubridJson)
	if !ok {
		t.Fatalf("expected *CubridJson, got %T", val)
	}
	if j.String() != `{"key":"value"}` {
		t.Fatalf("got %q", j.String())
	}
}

func TestEncodeBindValueJson(t *testing.T) {
	j := NewCubridJson(`[1,2,3]`)
	data, cubType, err := encodeBindValue(j)
	if err != nil {
		t.Fatal(err)
	}
	if cubType != protocol.CubridTypeString {
		t.Fatalf("type: got %d, want %d", cubType, protocol.CubridTypeString)
	}
	if string(data) != "[1,2,3]\x00" {
		t.Fatalf("data: got %q", data)
	}
}

func TestScanTypeForJson(t *testing.T) {
	got := scanTypeForCubridType(protocol.CubridTypeJSON)
	want := reflect.TypeOf(&CubridJson{})
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}
