package btree

import (
	"fmt"
	"os"
)

type KV struct {
    Path string
    // internals
    fp *os.File
    tree BTree
    mmap struct {
        file int // file size, can be larger than database size
        total int // mmap size, can be larger than file size
        chunks [][]byte // multiple mmaps, can be non-continuous
    }
    page struct {
        flushed uint64 // database size in number of pages
        temp [][]byte // newly allocated pages
    }
}

// 1. open a database
func (db *KV) Open() error {
    // open or create the DB file
    fp, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return fmt.Errorf("OpenFile: %w", err)
    }
    db.fp = fp

    // create the initial mmap
    sz, chunk, err := mmapInit(db.fp)
    if err != nil {
        db.Close()
        return fmt.Errorf("KV.Open: %w", err)
    }
    db.mmap.file = sz
    db.mmap.total = len(chunk)
    db.mmap.chunks = [][]byte{chunk}

    // Setting up btree callbacks
    db.tree.get = db.pageGet
    db.tree.new = db.pageNew
    db.tree.del = db.pageDel

    // read the master page
    err = masterLoad(db)
    if err != nil {
        db.Close()
        return fmt.Errorf("KV.Open : %w", err)
    }

    // done 
    return nil
}

// 2. close a database
// cleanups
func (db *KV) Close() {

}

// FIX: 3. read the db
// func (db *KV) Get(key []byte) ([]byte, bool) {
//     return db.tree.Get(key)
// }

// update the db
func (db *KV) Set(key, val []byte) error {
    db.tree.Insert(key, val)
    return flushPages(db)
}

func (db *KV) Del(key []byte) (bool, error) {
    deleted := db.tree.Delete(key)
    return deleted, flushPages(db)
}

// persist the newly allocated pages after updates
func flushPages(db *KV) error {
    if err := writePages(db); err != nil {
        return err
    }
    return syncPages(db)
}

func writePages(db *KV) error {
    // extend the file & mmap based on requirement
    npages := int(db.page.flushed) + len(db.page.temp)
    // if err := extendFile(db, npages); err != nil {
    //     return err
    // }

    if err := extendMmap(db, npages); err != nil {
        return err
    }
    // copy data to the file
    for i, page := range db.page.temp {
        ptr := db.page.flushed + uint64(i)
        copy(db.pageGet(ptr).data, page)
    }
    return nil
}

func syncPages(db *KV) error {
    // flush data to the disk. Must be done before updarting the master page
    if err := db.fp.Sync(); err != nil {
        return fmt.Errorf("fsync: %w", err)
    }
    db.page.flushed += uint64(len(db.page.temp))
    db.page.temp = db.page.temp[:0]

    // update and flush the master page
    if err := masterStore(db); err != nil {
        return err
    }
    if err := db.fp.Sync(); err != nil {
        return fmt.Errorf("fsync: %w", err)
    }
    return nil
}


func (db *KV) pageNew(node BNode) uint64 {
    // TODO : reuse deallocated pages
    assert(len(node.data) <= BTREE_PAGE_SIZE, "node.data length greater than page size")
    ptr := db.page.flushed + uint64(len(db.page.temp))
    db.page.temp = append(db.page.temp, node.data)
    return ptr
}

func (db *KV) pageDel(uint64) {
    // TODO : to be implemented later
}


// callback for BTree, dereference a pointer
func (db *KV) pageGet(ptr uint64) BNode {
    start := uint64(0)
    for _, chunk := range db.mmap.chunks {
        end := start + uint64(len(chunk)) / BTREE_PAGE_SIZE
        if ptr < end {
            offset := BTREE_PAGE_SIZE * (ptr - start)
            return BNode{chunk[offset : offset + BTREE_PAGE_SIZE]}
        }
        start = end
    }
    // If it reaches the function end and there is nothing to return
    // then you can panic
    panic("bad ptr")
}

