package ast

// Query represents a PRQL pipeline starting with a source.
type Query struct {
	From     Source
	Steps    []Step
	Target   string // optional target (e.g., sql.generic)
	Bindings []Binding
}

// Source represents a relation source.
type Source struct {
	Table string
	Rows  []InlineRow // inline rows when Table is empty
}

type InlineRow struct {
	Fields []Field
}

type Field struct {
	Name string
	Expr Expr
}

// Binding represents a named sub-query defined via `let`.
type Binding struct {
	Name  string
	Query *Query
}

// Step is a pipeline stage.
type Step interface {
	isStep()
}

type (
	FilterStep struct {
		Expr Expr
	}
	DeriveStep struct {
		Assignments []Assignment
	}
	Assignment struct {
		Name string
		Expr Expr
	}
	SelectStep struct {
		Items []SelectItem
	}
	SelectItem struct {
		Expr Expr
		As   string // optional alias
	}
	AggregateStep struct {
		Items []AggregateItem
	}
	AggregateItem struct {
		Func string
		Arg  Expr
		Args []Expr
		As   string
	}
	TakeStep struct {
		Limit  int
		Offset int
	}
	AppendStep struct {
		Query *Query
	}
	RemoveStep struct {
		Query *Query
	}
	LoopStep struct {
		Body []Step
	}
	JoinStep struct {
		Side  string
		Query *Query
		On    Expr
	}
	DistinctStep struct{}
	GroupStep    struct {
		Key   Expr
		Steps []Step
	}
	SortStep struct {
		Items []SortItem
	}
	SortItem struct {
		Expr Expr
		Desc bool
	}
)

func (*FilterStep) isStep()    {}
func (*DeriveStep) isStep()    {}
func (*SelectStep) isStep()    {}
func (*AggregateStep) isStep() {}
func (*TakeStep) isStep()      {}
func (*AppendStep) isStep()    {}
func (*RemoveStep) isStep()    {}
func (*LoopStep) isStep()      {}
func (*JoinStep) isStep()      {}
func (*GroupStep) isStep()     {}
func (*SortStep) isStep()      {}
func (*DistinctStep) isStep()  {}

// Expr is an expression node.
type Expr interface {
	isExpr()
}

type (
	Ident struct {
		Parts []string
	}
	Number struct {
		Value string
	}
	StringLit struct {
		Value string
	}
	Binary struct {
		Op    string
		Left  Expr
		Right Expr
	}
	Unary struct {
		Op   string
		Expr Expr
	}
	Call struct {
		Func Expr
		Args []Expr
	}
	Pipe struct {
		Input Expr
		Func  Expr
		Args  []Expr
	}
	CaseExpr struct {
		Branches []CaseBranch
	}
	CaseBranch struct {
		Cond  Expr
		Value Expr
	}
	Tuple struct {
		Exprs []Expr
	}
)

func (*Ident) isExpr()     {}
func (*Number) isExpr()    {}
func (*StringLit) isExpr() {}
func (*Binary) isExpr()    {}
func (*Unary) isExpr()     {}
func (*Call) isExpr()      {}
func (*Pipe) isExpr()      {}
func (*CaseExpr) isExpr()  {}
func (*Tuple) isExpr()     {}
