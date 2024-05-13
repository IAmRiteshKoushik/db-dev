package parser

import(
    "github.com/IAmRiteshKoushik/db-dev/cmd"
)

// common structure for queries: INDEX BY, FILTER, LIMIT
type QLScan struct{
    Table string
    // INDEX BY xxx
    Key1 QLNode // comparison, optional
    Key2 QLNode // comparison, optional
    // FILTER xxx
    Filter QLNode // boolean, optional
    // LIMIT x, y
    Offset int64
    Limit  int64
}

// stmt: select
type QLSelect struct {
    QLScan
    Names []string // expr AS name
    Output []QLNode // expression list
}

// stmt: update
type QLUpdate struct {
    QLScan
    Names   []string
    Values  []QLNode
}

// stmt: insert
type QLInsert struct {
    Table   string
    Mode    int
    Names   []string
    Values  [][]QLNode
}

// stmt: delete
type QLDelete struct {
    QLScan
}

// stmt: create table
type QLCreateTable struct {
    Def cmd.TableDef
}

func pStmt(p *Parser) interface{} {
    switch{
    case pKeyword(p, "create", "table"):
        return pCreateTable(p)
    case pKeyword(p, "select"):
        return pSelect(p)
    case pKeyword(p, "insert", "into"):
        return pInsert(p, cmd.MODE_INSERT_ONLY)
    case pKeyword(p, "replace", "into"):
        return pInsert(p, cmd.MODE_UPDATE_ONLY)
    case pKeyword(p, "upsert", "into"):
        return pInsert(p, cmd.MODE_UPSERT)
    case pKeyword(p, "delete", "from"):
        return pDelete(p)
    case pKeyword(p, "update"):
        return pUpdate(p)
    default:
        pErr(p, nil, "unknown stmt")
        return nil
    }
}
