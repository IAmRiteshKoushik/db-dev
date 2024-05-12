package btree

// Now, B-tree is immutable: every update to the KV store will create new node
// in the path instead of updating current nodes, leaving some nodes 
// unreachable. We need to reuse these unreachable nodes from old versions 
// else DB grows infinitely

const BNODE_FREE_LIST = 3
const FREE_LIST_HEADER = 4 + 8 + 8
const FREE_LIST_CAP = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8

type FreeList struct {
    head uint64
    // callbacks for managing on-disk pages
    get func(uint64) BNode  // dereference a pointer
    new func(BNode) uint64  // append a new page
    use func(uint64, BNode) // reuse a page
}

// number of items in the list
func (fl *FreeList) Total() int

// get the nth pointer
func (fl *FreeList) Get(topn int) uint64

// remove pointers and add some new pointers
func (fl *FreeList) Update(popn int, freed []uint64)

// Functions for accessing the list node:
func flnSize(node BNode) int
func flnNext(node BNode) uint64
func flnPtr(node BNode, idx int)
func flnSetPtr(node BNode, idx int, ptr uint64)
func flnSetHeader(node BNode, size uint16, next uint64)
func flnSetTotal(node BNode, total uint64)


