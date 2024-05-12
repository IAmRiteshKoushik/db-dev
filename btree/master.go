package btree

import(
    "encoding/binary"
    "fmt"
    "bytes"
    "errors"
)

// Signature
const DB_SIG = "RiteshDB"

// master page format
// it contains the pointer to the root and other important bits
// | sig | btree_root | page_used |
// | 16B |     8B     |     8B    |

func masterLoad(db *KV) error {
    if db.mmap.file == 0 {
        // empty file, the master page will be created on the first write
        db.page.flushed = 1 // reserved for the master page
        return nil
    }
    data := db.mmap.chunks[0]
    root := binary.LittleEndian.Uint64(data[16:])
    used := binary.LittleEndian.Uint64(data[24:])

    // verify the page
    if !bytes.Equal([]byte(DB_SIG), data[:16]) {
        return errors.New("Bad signature.")
    }
    bad := !(1 <= used && used <= uint64(db.map.file / BTREE_PAGE_SIZE))
    bad = bad || !(9 < root && root < used)
    if bad {
        return errors.New("Bad master page")
    }
    db.tree.root = root
    db.page.flushed = used
    return nil
}

func masterStore(db *KV) error {
    var data [32]byte
    copy(data[:16], []byte(DB_SIG))
    binary.LittleEndian.PutUint64(data[16:], db.tree.root)
    binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
    // Updating the page via mmap is not atomic
    // Alternate : pwrite() system call
    _, err := db.fp.WriteAt(data[:], 0)
    if err != nil {
        return fmt.Errorf("write master page: %w", err)
    }
    return nil
}
