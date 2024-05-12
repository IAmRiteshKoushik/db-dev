package btree

// So far, the btree has been in-memory
// Now, we need to find a way to dump it to disk

import (
	"fmt"
	"os"
	"syscall"
    "errors"
)

// create the initial mmap that covers the whole file
func mmapInit(fp *os.File) (int, []byte, error) {
    fi, err := fp.Stat()
    if err != nil {
        return 0, nil, fmt.Errorf("stat: %w", err)
    }
    if fi.Size() % BTREE_PAGE_SIZE != 0 {
        return 0, nil, errors.New("File size is not a multiple of page size.")
    }
    mmapSize := 64 << 20
    assert(mmapSize % BTREE_PAGE_SIZE == 0, "mmapSize not multiple of page size")
    for mmapSize < int(fi.Size()) {
        mmapSize *= 2
    }

    // mmapSize can be larger than the file
    chunk, err := syscall.Mmap(int(fp.Fd()), 0, mmapSize, 
        syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
    if err != nil {
        return 0, nil, fmt.Errorf("mmap: %w", err)
    }
    return int(fi.Size()), chunk, nil
}


func extendMmap(db *KV, npages int) error {
    if db.mmap.total >= npages * BTREE_PAGE_SIZE {
        return nil
    }

    // double the address space
    chunk, err := syscall.Mmap(
        int(db.fp.Fd()), int64(db.mmap.total), db.mmap.total,
        syscall.PROT_READ | syscall.PROT_WRITE, syscall.MAP_SHARED)
    if err != nil {
        return fmt.Errorf("mmap: %w", err)
    }
    db.mmap.total += db.mmap.total
    db.mmap.chunks = append(db.mmap.chunks, chunk)
    return nil

}

// used by writePages()
// func extendFile(db *KV, npages int) error {
//
// }
