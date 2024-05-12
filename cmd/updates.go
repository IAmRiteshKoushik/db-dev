package cmd

// operation modes
const(
    MODE_UPSERT      = 0 // insert or replace
    MODE_UPDATE_ONLY = 1 // update existing keys   
    MODE_INSERT_ONLY = 2 // only add new keys
)

type InsertReq struct { tree    *BTree
    // out
    Added   bool // added a new key
    // in
    Key     []byte
    Val     []byte
    Mode    int
}

func (tree *BTree) InsertEx(req *InsertReq)
func (db *KV) Update(key []byte, val []byte, mode int) (bool, error)

// add a row to the table
func dbUpdate(db *DB, tdef *TableDef, rec Record, mode int) (bool, error) {

}

// add a record 
func (db *DB) Set(table string, rec Record, mode int) (bool, error) {

}
func (db *DB) Insert(table string, rec Record) (bool, error) {

}
func (db *DB) Update(table string, rec Record) (bool, error) {

}
func (db *DB) Upsert(table string, rec Record) (bool, error) {

}

