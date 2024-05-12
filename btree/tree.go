package btree

import "encoding/binary"

type BNode struct {
    data []byte // to be dumped to disk
}

const(
    BNODE_NODE = 1 // internal node (no value)
    BNODE_LEAF = 2 // leaf node (with value)
)

// header
func (node BNode) btype() uint16 {
    return binary.LittleEndian.Uint16(node.data)
}
func (node BNode) nkeys() uint16 {
    return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
    binary.LittleEndian.PutUint16(node.data[0:2], btype)
    binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// pointers
func (node BNode) getPtr(idx uint16) uint64 {
    assert(idx < node.nkeys(), "idx not less than nkeys") 
    pos := HEADER + 8 * idx
    return binary.LittleEndian.Uint64(node.data[pos:])
}
func (node BNode) setPtr(idx uint16, val uint64) {
    assert(idx < node.nkeys(), "idx not less than nkeys") 
    pos := HEADER + 8 * idx
    binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// offset list - used to locate nth KV pair quickly
func offsetPos(node BNode, idx uint16) uint16 {
    assert(1 <= idx && idx <= node.nkeys(), "idx not in range (1, nkeys)")
    return HEADER + 8 * node.nkeys() + 2 * (idx - 1)
}
func (node BNode) getOffSet(idx uint16) uint16 {
    if idx == 0 {
        return 0
    }
    return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}
func (node BNode) setOffset(idx uint16, offset uint16){
    binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

// key-values (KV)
func (node BNode) kvPos(idx uint16) uint16 {
    assert(idx <= node.nkeys(), "idx not less than nkeys")
    return HEADER + 8 * node.nkeys() + 2 * node.nkeys() + node.getOffSet(idx)
}
func (node BNode) getKey(idx uint16) []byte {
    assert(idx < node.nkeys(), "idx not less than nkeys")
    pos := node.kvPos(idx)
    klen := binary.LittleEndian.Uint16(node.data[pos:])
    return node.data[pos + 4:][:klen]
}
func (node BNode) getVal(idx uint16) []byte {
    assert(idx < node.nkeys(), "idx not less than nkeys")    
    pos := node.kvPos(idx)
    klen := binary.LittleEndian.Uint16(node.data[pos + 0:])
    vlen := binary.LittleEndian.Uint16(node.data[pos + 2:])
    return node.data[pos + 4 + klen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
    return node.kvPos(node.nkeys())
}

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
    assert(node1max > BTREE_MAX_KEY_SIZE, "Node size exceeds allowed limit")

    // if node1max > BTREE_PAGE_SIZE {
        // Handle error: Node size exceeds page size limit
        // assertion did not work properly

        // panic("Node size exceeds allowed limit")
        // assert.True(node1max <= BTREE_PAGE_SIZE)
    // }
}

func assert(condition bool, msg string){
    if condition != true{
        panic(msg)
    }
}

func (tree *BTree) Delete(key []byte) bool {
    assert(len(key) != 0, "Key length is 0")
    assert(len(key) <= BTREE_MAX_KEY_SIZE, "Key-length exceeded MAX_SIZE")
    if tree.root == 0 {
        return false
    }
    updated := treeDelete(tree, tree.get(tree.root), key)
    if len(updated.data) == 0 {
        return false // not found
    }
    tree.del(tree.root)
    if updated.btype() == BNODE_NODE && updated.nkeys() == 1 {
        // remove a level
        tree.root = updated.getPtr(0)
    } else {
        tree.root = tree.new(updated)
    }
    return true
}

func (tree *BTree) Insert(key []byte, val []byte) {
    assert(len(key) != 0, "")
    assert(len(key) <= BTREE_MAX_KEY_SIZE, "Key-length exceeded MAX_SIZE")
    assert(len(val) <= BTREE_MAX_VAL_SIZE, "Key-length exceeded MAX_SIZE")

    if tree.root == 0 {
        // create the first node
        root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
        root.setHeader(BNODE_LEAF, 2)
        // a dummy key, this makes the tree cover the whole key space
        // thus a lookcup can always find a containing node
        nodeAppendKV(root, 0, 0, nil, nil)
        nodeAppendKV(root, 1, 0, key, val)
        tree.root = tree.new(root)
        return
    }
    node := tree.get(tree.root)
    tree.del(tree.root)

    node = treeInsert(tree, node, key, val)
    nsplit, splitted := nodeSplit3(node)
    if nsplit > 1 {
        root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
        root.setHeader(BNODE_NODE, nsplit)
        for i, knode := range splitted[:nsplit] {
            ptr, key := tree.new(knode), knode.getKey(0)
            nodeAppendKV(root, uint16(i), ptr, key, nil)
        }
        tree.root = tree.new(root)
    } else {
        tree.root = tree.new(splitted[0])
    }
}
