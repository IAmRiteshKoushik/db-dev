package parser

// syntax tree
type QLNode struct {
    Value // Type I64, Str
    Kids []QLNode
}

type Parser struct {
    input   []byte
    idx     int
    err     error
}

func pExprTuple(p *Parser, node *QLNode) {
    kids := []QLNode{{}}
    pExprOr(p, &kids[len(kids) - 1])
}

func pExprOr(p *Parser, node *QLNode){

}

func pExprAnd(p *Parser, node *QLNode){

}

func pExprNot(p *Parser, node *QLNode){ // NOT a

} 

func pExprCmp(p *Parser, node *QLNode){

}

func pExprAdd(p *Parser, node *QLNode){

}

func pExprMul(p *Parser, node *QLNode){

}

func pExprUnop(p *Parser, node *QLNode){

}

func pExprBinop(p *Parser, node *QLNode, ops []string, types []uint32, 
    next func(*Parser, *QLNode)){
    
}

func pKeyword(p *Parser, kwds ...string) bool {

}

func pExprAtom(p *Parser, node *QLNode){

}

func pErr(){

}

func pSym(){

}


func isSum(ch byte) bool {

}

func isSymStart(ch byte) bool {

}



