package cmd

// Retriving a range of records (range query)

type BIter struct {
    tree *BTree
    path []BNode // from root to leaf
    pos []uint16 // indexes into nodes
}


