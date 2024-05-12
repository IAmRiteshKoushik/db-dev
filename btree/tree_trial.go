package btree

import(
    "unsafe"
)

// Container code for testing the B-tree. It keeps pages in an in-memory
// hashmap without persisting them to the disk. Persistance to be 
// included later
type Container struct {
    tree BTree
    ref map[string]string
    pages map[uint64]BNode
}

func newContainer() *Container {
    pages := map[uint64]BNode{}
    return &Container{
        tree: BTree{
            get: func(ptr uint64) BNode {
                node, ok := pages[ptr]
                assert(ok, "Page not found in get()")
                return node
            },
            new: func(node BNode) uint64 {
                assert(node.nbytes() <= BTREE_PAGE_SIZE, "")
                key := uint64(uintptr(unsafe.Pointer(&node.data[0])))
                assert(pages[key].data == nil, "data not nil, cannot use new()")
                pages[key] = node
                return key
            },
            del: func(ptr uint64) {
                _, ok := pages[ptr]
                assert(ok, "Page not found in del()")
                delete(pages, ptr)
            },
        },
        // Returning an empty reference map
        ref: map[string]string{},
        // Returning an empty pages map
        pages: pages,
    }
}

func (c *Container) add(key string, val string){
    c.tree.Insert([]byte(key), []byte(val))
    c.ref[key] = val
}

func (c *Container) del(key string) bool {
    delete(c.ref, key)
    return c.tree.Delete([]byte(key))
}
