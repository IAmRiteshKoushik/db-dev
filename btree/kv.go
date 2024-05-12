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

        // TO FIX
        // temp [][]byte // newly allocated pages

        // newly allocated or deallocated pages keyed by the pointer
        // nil value denotes a deallocated page
        nfree int
        nappend int
        updates map[uint64][]byte
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
    // update the free list
    freed := []uint64{}
    for ptr, page := range db.page.updates {
        if page == nil {
            freed = append(freed, ptr)
        }
    }

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

    // copy pages to the file
    for ptr, page := range db.page.updates {
        if page != nil {
            copy(pageGetMapped(db, ptr).data, page)
        }
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


// func (db *KV) pageNew(node BNode) uint64 {
//     // TODO : reuse deallocated pages
//     assert(len(node.data) <= BTREE_PAGE_SIZE, "node.data length greater than page size")
//     ptr := db.page.flushed + uint64(len(db.page.temp))
//     db.page.temp = append(db.page.temp, node.data)
//     return ptr
// }

// func (db *KV) pageDel(uint64) {
//     // TODO : to be implemented later
// }

// callback for BTree, allocate a new page
func (db *KV) pageNew(node BNode) uint64 {
    assert(len(node.data) <= BTREE_PAGE_SIZE)
    ptr := uint64(0)
    if db.page.nfree < db.free.Total() {
        // reuse a deallocated page
        ptr = db.free.Get(db.page.nfree)
        db.page.nfree++
    } else {
        // append a new page
        ptr = db.page.flushed + uint64(db.page.nappend)
        db.page.nappend++
    }
    db.page.updates[ptr] = node.data
    return ptr
}


// callback for BTree to deallocate a page
func (db *KV) pageDel(ptr uint64) {
    db.page.updates[ptr] = nil
}

func (db *KV)pageGet(ptr uint64) BNode {
    if page, ok := db.page.updates[ptr]; ok {
        assert(page != nil, "page does not exist")
        return BNode{page} // for new pages
    }
    return pageGetMapped(db, ptr) // for written pages
}


// callback for BTree, dereference a pointer
// this function was previously pageGet 
func pageGetMapped(db *KV, ptr uint64) BNode {
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

// callback for Freelist, allocate a new page
func (db *KV) pageAppend(node BNode) uint64 {
    assert(len(node.data) <= BTREE_PAGE_SIZE, "node-data more than MAX_PAGE_SIZE")
    ptr := db.page.flushed + uint64(db.page.nappend)
    db.page.nappend++
    db.page.updates[ptr] = node.data
    return ptr
}

func (db *KV) pageUse(ptr uint64, node BNode) {
    db.page.updates[ptr] = node.data
}
