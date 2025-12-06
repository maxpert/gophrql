package ast

// Query represents a PRQL pipeline starting with a source.
type Query struct {
	From   Source
	Steps  []Step
	Target string // optional target (e.g., sql.generic)
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
		As   string
	}
	TakeStep struct {
		Limit  int
		Offset int
	}
	AppendStep struct {
		Query *Query
	}
	GroupStep struct {
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
func (*GroupStep) isStep()     {}
func (*SortStep) isStep()      {}

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
	Call struct {
		Func Expr
		Args []Expr
	}
	Pipe struct {
		Input Expr
		Func  Expr
		Args  []Expr
	}
)

func (*Ident) isExpr()     {}
func (*Number) isExpr()    {}
func (*StringLit) isExpr() {}
func (*Binary) isExpr()    {}
func (*Call) isExpr()      {}
func (*Pipe) isExpr()      {}
