package btree

import(
    "assert"
)

type BNode struct {
    data []byte // to be dumped to disk
}

const(
    BNODE_NODE = 1 // internal node (no value)
    BNODE_LEAF = 2 // leaf node (with value)
)

type BTree struct {
    // pointer (a non-zero page number)
    root    uint64
    // callbacks for managing on-disk pages
    get     func(uint64) BNode
    new     func(BNode) uint64
    del     func(uint64)
}

const HEADER = 4
const BTREE_PAGE_SIZE = 4096    // page size is defined to be 4KiB
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

func init() {
    node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
    assert(node1max <= BTREE_PAGE_SIZE)
}

// header
func (node BNode) btype() uint16 {

}
func (node BNode) nkeys() uint16 {

}

func (node BNode) setHeader(btype uint16, nkeys uint16) {

}

// pointers
func (node BNode) getPtr(idx uint16) uint64 {

}
func (node BNode) setPtr(idx uint16, val uint64) {

}

// offset list - used to locate nth KV pair quickly
func offsetPos(node BNode, idx uint16) uint16 {

}
func (node BNode) getOffSet(idx uint16) uint16 {

}

// key-values (KV)
func (node BNode) kvPos(idx uint16) uint16 {

}
func (node BNode) getKey(idx uint16) []byte {

}
func (node BNode) getVal(idx uint16) []byte {

}

// node size in bytes
func (node BNode) nbytes() uint16 {

}

// B-Tree Insertion
