package btree

import "encoding/binary"

type BNode struct {
    data []byte // to be dumped to disk

    // The []byte slice consists for the following
    // type - 2B (input - uint16)
    // nkeys - 2B (input - uint16)
    // pointers - n * 8B (input - uint64) (n = nkeys)
    // offsets - n * 2B (input - ) (n = nkeys)
    // KV - ...
    
    // The format for KV is as follows:
    // klen - 2B (input - uint16)
    // vlen - 2B (input - uint16)
    // key - 1000B (input - int64, []byte) 
    // val - 3000B (input - int64, []byte)
}

const(
    BNODE_NODE = 1 // internal node (no value)
    BNODE_LEAF = 2 // leaf node (with value)
)

// 1. header
// Adding the header to the []byte slice in LittleEndian encoding
// format. If we directly add uint64 then it will occupy more space 
// and we have been trying to limit things to 2B hence uint16 format
// works for both encoding and decoding (setting and getting headers)
func (node BNode) btype() uint16 {
    // GETTER METHOD :
    // Returns the first two bytes after converting in uint16 format
    return binary.LittleEndian.Uint16(node.data)
}
func (node BNode) nkeys() uint16 {
    // GETTER METHOD :
    // Returns the second and third bits after converting in uint16 format
    return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
    // SETTER METHOD :
    // PutUint16 function, accepts a uint16 number because this number
    // can be stored in 16 bits (or) 2 bytes of memory and PutUint16 
    // by default changes 2 bytes in a []byte. So, the number gets 
    // converted and stored in the first two locations
    binary.LittleEndian.PutUint16(node.data[0:2], btype)
    binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// 2. pointers
func (node BNode) getPtr(idx uint16) uint64 {
    // GETTER METHOD :
    // A pointer is uint64 type in Go. idx is where the pointer is stored in []byte
    // We are storing all pointers in the []byte slice. 
    // Space alloc : nkeys * 8B
    // idx starts from 0. So first pointer position starts from HEADER = [4:]
    // LittleEndian.Uint64() starts at zero-th position of given byte-slice and 
    // traverses 8 bytes worth of data. Then converts to uint64 and returns
    assert(idx < node.nkeys(), "idx not less than nkeys") 
    // idx must be lesser than nkeys() because we beging indexing from 0
    pos := HEADER + 8 * idx
    return binary.LittleEndian.Uint64(node.data[pos:])
}
func (node BNode) setPtr(idx uint16, val uint64) {
    // SETTER METHOD :
    // Setter works similar to Getter where we first move to the pointer 
    // position using idx and then we overwrite existing data in the []byte 
    // slice for 8 bytes with the provided "val" which is a uint64 pointer
    // This is how new pointers replace old pointers
    assert(idx < node.nkeys(), "idx not less than nkeys") 
    // idx must be lesser than nkeys() because we begin indexing from 0
    pos := HEADER + 8 * idx
    binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// offset list - used to locate nth KV pair quickly
func offsetPos(node BNode, idx uint16) uint16 {
    // This method is neither a Setter nor a Getter.
    // This is used to determine the offset-position
    // which is in-turn used to locate the actual offset

    assert(1 <= idx && idx <= node.nkeys(), "idx not in range (1, nkeys)")
    // The offset for idx = 0 is 0 and this function does not handle it
    // That is handled by the getOffset function. Also, idx <= node.nkeys()
    // because number of offsets = number of KV-pairs.
    // In m-way trees the "key" = (m-1) and "value" = m. In our content 
    // "key" refers to "pointers" and "value" refers to "KV pair"

    // For returning the offset:
    // Go across the HEADER 
    // Go across the POINTERS-LIST
    // Go across Offset = (idx - 1) * 2B
    return HEADER + 8 * node.nkeys() + 2 * (idx - 1)
}
func (node BNode) getOffSet(idx uint16) uint16 {
    // GETTER METHOD :
    // This method utilizes the offsetPos calculator function
    // and from the determined position of the offset in the []byte slice
    // we locate the actual offset. This is like a pointer to the offset 
    // position

    // If idx = 0, then there is no offset
    if idx == 0 {
        return 0
    }
    // If idx > 0, then there is offset and it is determined by the following
    return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}
func (node BNode) setOffset(idx uint16, offset uint16){
    // SETTER METHOD: 
    // An offset is a pointer to a KV pair, so we are using uint16 as the 
    // data for the input. As offsetPos() is not a struct method, it requires
    // us to pass the node.
    binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

// key-values (KV)
func (node BNode) kvPos(idx uint16) uint16 {
    // GETTER METHOD:
    // We need to locate the position of kv in order to find it from the 
    // []byte slice.
    assert(idx <= node.nkeys(), "idx not less than nkeys")
    // idx must be lesser or equal to nkeys() because: 
    // We are using B+ Tree which contains (m-1) keys* and (m) values*
    // *keys = pointers
    // *values = KV pair

    // For returning kvPos:
    // Cross of the HEADER - type, keys-list = 4B
    // Cross over the POINTERS-LIST - nkeys() * 8B
    // Cross over the OFFSET-LIST - nkeys() * 2B
    // Add in the offset - xB
    return HEADER + 8 * node.nkeys() + 2 * node.nkeys() + node.getOffSet(idx)
}
func (node BNode) getKey(idx uint16) []byte {
    assert(idx < node.nkeys(), "idx not less than nkeys")

    // Locating position of kv
    pos := node.kvPos(idx)

    // As KV is part of the []byte slice, we need to find the length of the 
    // key in order to locate the key inside the []byte
    // Design of KV - | klen(2B) | vlen(2B) | key(1000B) | value(3000B)
    klen := binary.LittleEndian.Uint16(node.data[pos:])
    // After getting the length of the key, we need to start a search from
    // after vlen i.e (klen + vlen = 4B)
    // We reduce the search space to starting byte of KV to last byte of node
    // which itself is a []byte. Now, we need to take a slice of that as well
    // so that we only get the "key". We do that by slicing from start to 
    // length of key.
    return node.data[pos + 4:][:klen]
}
func (node BNode) getVal(idx uint16) []byte {
    assert(idx < node.nkeys(), "idx not less than nkeys")    

    // Locating position of kv
    pos := node.kvPos(idx)

    // As KV is part of the []byte slice, we need to find the length of the 
    // key and value in order to locate the value inside the []byte 
    // Design of KV - | klen(2B) | vlen(2B) | key(1000B) | value(3000B)
    klen := binary.LittleEndian.Uint16(node.data[pos + 0:])
    vlen := binary.LittleEndian.Uint16(node.data[pos + 2:])

    // Starting location of value -> location of KV-pair + (klen + vlen = 4B) + key
    // Ending location of value is -> taking a slice from the starting location 
    // and ending at length of value-length(vlen). The reason why we are not 
    // going to the end of the []byte slice is because there are multiple 
    // keys, values stored inside it and we need to slice appropriately
    return node.data[pos + 4 + klen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
    // GETTER METHOD:
    // In order to get the size of the node, we can simply calculate the last 
    // position where a KV is to be inserted. As the position would tell us the 
    // size because size = lenght of []byte stream
    return node.kvPos(node.nkeys())

    // Alternately, we can think of the len() function over the []bytes slice
    // and then converting that to uint16. But as we are using a pure-DS
    // approach, we have avoided utilizing built-ins as much as possible
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

// Function to check is max_node_size remains smaller than page size
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
    assert(len(key) != 0, "key-length not equal to 0")
    assert(len(key) <= BTREE_MAX_KEY_SIZE, "Key-length exceeded MAX_SIZE")
    assert(len(val) <= BTREE_MAX_VAL_SIZE, "value-length exceeded MAX_SIZE")

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
