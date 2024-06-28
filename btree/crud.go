package btree

import (
	"bytes"
	"encoding/binary"
)

// IDEA :
// 1. LOOK UP THE KEY
    // In order to insert a key into a leaf node, we need to look up its 
    // position in the sorted KV list
// 2. UPDATE LEAF NODES
    // After looking up the position to insert/update, we need to create a copy
    // of the node with the new key in it
// 3. RECURSIVE INSERTION
// 4. HANDLE INTERNAL NODES
// 5. SPLIT BIG NODES
// 6. UPDATE INTERNAL NODES


func nodeLookupLE(node BNode, key []byte) uint16 {
    nkeys := node.nkeys()
    found := uint16(0)
    // the first key is a copy of the parent node
    // thus it is always less than or equal to the key
    for i := uint16(1); i < nkeys; i++ {
        cmp := bytes.Compare(node.getKey(i), key)
        if cmp <= 0 {
            found = i
        }
        if cmp >= 0 {
            break
        }
    }
    return found
}

func leafInsert(newNode, old BNode, idx uint16, key, val []byte) {
    newNode.setHeader(BNODE_LEAF, old.nkeys() + 1)
    nodeAppendRange(newNode, old, 0, 0, idx)
    nodeAppendKV(newNode, idx, 0, key, val)
    nodeAppendRange(newNode, old, idx + 1, idx, old.nkeys() - idx)
}

func leafUpdate(newNode, old BNode, idx uint16, key, val []byte) {
    // TODO : The function is supposedly similar to leafInsert
    newNode.setHeader(BNODE_LEAF, old.nkeys() + 1)
    nodeAppendRange(newNode, old, 0, 0, idx)
    nodeAppendKV(newNode, idx, 0, key, val)
    nodeAppendRange(newNode, old, idx + 1, idx, old.nkeys() - idx)
}

// copy multiple KVs into the position
func nodeAppendRange(newNode, old BNode, dstNew, srcOld, n uint16){
    assert(srcOld + n <= old.nkeys(), "srcOld overflow") 
    assert(dstNew + n <= newNode.nkeys(), "dstNew overflow")
    if n == 0 {
        return
    }

    // pointers
    for i := uint16(0); i < n; i++ {
        newNode.setPtr(dstNew + i, old.getPtr(srcOld + i))
    }

    // offsets
    dstBegin := newNode.getOffSet(dstNew)
    srcBegin := old.getOffSet(srcOld)
    for i := uint16(1); i <= n; i++ {
        offset := dstBegin + old.getOffSet(srcOld + i) - srcBegin
        newNode.setOffset(dstNew + i, offset)
    }

    // KVs
    begin := old.kvPos(srcOld)
    end := old.kvPos(srcOld + n)
    copy(newNode.data[newNode.kvPos(dstNew):], old.data[begin:end])
}

func nodeAppendKV(newNode BNode, idx uint16, ptr uint64, key, val []byte) {
    // ptrs
    newNode.setPtr(idx, ptr) 

    // KVs
    pos := newNode.kvPos(idx)
    binary.LittleEndian.PutUint16(newNode.data[pos + 0:], uint16(len(key)))
    binary.LittleEndian.PutUint16(newNode.data[pos + 2:], uint16(len(val)))
    copy(newNode.data[pos + 4:], key)
    copy(newNode.data[pos + 4 + uint16(len(key)):], val)
    // the offset of the next key
    newNode.setOffset(idx + 1, newNode.getOffSet(idx) + 4 + uint16((len(key) + len(val))))
}

// 1. B-Tree Insertion
// insert a KV into a node, then result might be split into 2 nodes
// the called is responsible for deallocating the input node
// and splitting and reallocating result nodes
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
    // the result node - if bigger than 1 page -> splits
    newNode := BNode{data: make([]byte, 2 * BTREE_PAGE_SIZE)}

    // where to insert the key
    idx := nodeLookupLE(node, key)
    // act depending on the node type
    switch node.btype(){
    case BNODE_LEAF:
        // leaf, node.getKey(idx) <= key
        if bytes.Equal(key, node.getKey(idx)){
            // key found, update it.
            leafUpdate(newNode, node, idx, key, val)
        } else {
            // insert it after the position
            leafInsert(newNode, node, idx + 1, key, val)
        }
    case BNODE_NODE:
        // internal node, insert it to a child node
        nodeInsert(tree, newNode, node, idx, key, val)
    default:
        panic("bad node!")
    }
    return newNode
}

func nodeInsert(tree *BTree, newNode, node BNode, idx uint16, key, val []byte){
    // get and deallocate the kid node 
    kptr := node.getPtr(idx)
    knode := tree.get(kptr)
    tree.del(kptr)
    // recursive insertion to the kid node
    knode = treeInsert(tree, knode, key, val)
    // split the result
    nsplit, splited := nodeSplit3(knode)
    // update the kid links
    nodeReplaceKidN(tree, newNode, node, idx, splited[:nsplit]...)
}

// Splitting of nodes
// split a bigger-than-allowed node into two
// the seconde node always fits on a page
func nodeSplit2(left, right, old BNode) {
    
}

// split a node if it's too big. the results are 1~3 nodes
func nodeSplit3(old BNode) (uint16, [3]BNode) {
    if old.nbytes() <= BTREE_PAGE_SIZE {
        old.data = old.data[:BTREE_PAGE_SIZE]
        return 1, [3]BNode{old}
    }
    left := BNode{make([]byte, 2 * BTREE_PAGE_SIZE)} // might be split later
    right := BNode{make([]byte, BTREE_PAGE_SIZE)}
    nodeSplit2(left, right, old)
    if left.nbytes() <= BTREE_PAGE_SIZE {
        left.data = left.data[:BTREE_PAGE_SIZE]
        return 2, [3]BNode{left, right}
    }
    // the left node is still too large
    leftleft := BNode{make([]byte, BTREE_PAGE_SIZE)}
    middle := BNode{make([]byte, BTREE_PAGE_SIZE)}
    nodeSplit2(leftleft, middle, left)
    assert(leftleft.nbytes() <= BTREE_PAGE_SIZE, "leftleft.nbytes() not less than page size") 
    return 3, [3]BNode{leftleft, middle, right}
}

func nodeReplaceKidN(tree *BTree, newNode, old BNode, idx uint16, kids ...BNode) {
    inc := uint16(len(kids))
    newNode.setHeader(BNODE_NODE, old.nkeys() + inc - 1)
    nodeAppendRange(newNode, old, 0, 0, idx)
    for i, node := range kids {
        nodeAppendKV(newNode, idx + uint16(i), tree.new(node), node.getKey(0), nil)
    }
    nodeAppendRange(newNode, old, idx + inc, idx + 1, old.nkeys() - (idx + 1))
}

// The parameters of this function can have better name (esp. merged)
func nodeReplace2Kid(newNode, old BNode, idx uint16, merged uint64 ,key []byte){
    // TODO : Compelete the function
}

// 2. B-Tree Deletion
// remove a key from leaf node
func leafDelete(newNode BNode, old BNode, idx uint16){
    newNode.setHeader(BNODE_LEAF, old.nkeys() - 1)
    nodeAppendRange(newNode, old, 0, 0, idx)
    nodeAppendRange(newNode, old, idx, idx + 1, old.nkeys() - (idx + 1))
}

// Recursive Deletion - delete a key from the tree
func treeDelete(tree *BTree, node BNode, key []byte) BNode {
    // where is the key ?
    idx := nodeLookupLE(node, key)
    // depending on the type of node
    switch node.btype() {
    case BNODE_LEAF:
        if !bytes.Equal(key, node.getKey(idx)){
            return BNode{}  // not found
        }
        // delete the key in the leaf
        newNode := BNode{data: make([]byte, BTREE_PAGE_SIZE)}         
        leafDelete(newNode, node, idx)
        return newNode
    case BNODE_NODE:
        return nodeDelete(tree, node, idx, key)
    default:
        panic("bad node!")
    }
}

// part of the treeDelete()
func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
    // recursve into child node
    kptr := node.getPtr(idx)
    updated := treeDelete(tree, tree.get(kptr), key)
    if len(updated.data) == 0 {
        return BNode{} // not found
    }
    tree.del(kptr)

    newNode := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    // check for merging
    mergeDir, sibling := shouldMerge(tree, node, updated, idx)
    switch {
    case mergeDir < 0: // left
        merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)} 
        nodeMerge(merged, sibling, updated)
        tree.del(node.getPtr(idx - 1))
        nodeReplace2Kid(newNode, node, idx - 1, tree.new(merged), merged.getKey(0))
    case mergeDir > 0: // right
        merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)} 
        nodeMerge(merged, updated, sibling)
        tree.del(node.getPtr(idx + 1))
        nodeReplace2Kid(newNode, node, idx, tree.new(merged), merged.getKey(0))
    case mergeDir == 0:
        assert(updated.nkeys() > 0, "nkeys not greater than 0")
        nodeReplaceKidN(tree, newNode, node, idx, updated)
    }
    return newNode
}

// merge two nodes in 1
func nodeMerge(newNode, left, right BNode) {
    newNode.setHeader(left.btype(), left.nkeys() + right.nkeys())
    nodeAppendRange(newNode, left, 0, 0, left.nkeys())
    nodeAppendRange(newNode, right, left.nkeys(), 0, right.nkeys())
}

// Conditions for merging 
// 1. The node is smaller than 1/4 of a page(this is arbitrary)
// 2. Has a sibling and the merged result does not exceed one page

// should the updated child be merged with a sibling ?
func shouldMerge(tree *BTree, node, updated BNode, idx uint16) (int, BNode) {
    if updated.nbytes() > BTREE_PAGE_SIZE / 4 {
        return 0, BNode{}
    }
    if idx > 0 {
        sibling := tree.get(node.getPtr(idx - 1))
        merged := sibling.nbytes() + updated.nbytes() - HEADER
        if merged <= BTREE_PAGE_SIZE {
            return -1, sibling
        }
    }
    if idx + 1 < node.nkeys() {
        sibling := tree.get(node.getPtr(idx + 1))
        merged := sibling.nbytes() + updated.nbytes() - HEADER
        if merged <= BTREE_PAGE_SIZE {
            return 1, sibling
        }
    }
    return 0, BNode{}
}
