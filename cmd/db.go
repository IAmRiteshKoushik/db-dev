package cmd 

import(
    "github.com/IAmRiteshKoushik/btree/kv"
)

const(
    TYPE_ERROR = 0
    TYPE_BYTES = 1
    TPE_INT64 = 2
)

// table cell
type Value struct{
    Type uint32
    I64 int64
    Str []byte
}

// table row
type Record struct{
    Cols []string
    Vals []Value
}

func (rec *Record) AddStr(key string, val []byte) *Record
func (rec *Record) AddInt64(key string, val int64) *Record
func (rec *Record) Get(key string) *Value

type DB struct {
    Path string
    // internals
    // TODO: To be fixed later
    // kv KV -> 
    tables map[string]*TableDef // cached table definition
}

type TableDef struct {
    // user defined
    Name    string
    Types   []uint32    // column types
    Cols    []string    // column names
    PKeys   int         // the first PKeys columns are the  primary key
    // auto-assigned B-tree prefixes for different tables
    // To support multiple tables, the keys in KV store are prefixed with 
    // unique 32-bit number
    Prefix  uint32
}

// For storing table definitions (which is metadata)
// internal table: metadata
var TDEF_META = &TableDef{
    Prefix: 1,
    Name:   "@meta",
    Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
    Cols:   []string{"key", "val"},
    PKeys:  1,
}

// internal table: table schemas
var TDEF_TABLE = &TableDef{
    Prefix: 2,
    Name:   "@table",
    Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
    Cols:   []string{"name", "def"},
    PKeys:  1,
}
