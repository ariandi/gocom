package excel

type ExcelColumn int

const (
	A ExcelColumn = iota + 1
	B
	C
	D
	E
	F
	G
	H
	I
	J
	K
	L
	M
	N
	O
	P
	Q
	R
	S
	T
	U
	V
	W
	X
	Y
	Z
)

func ToChar(i int) rune {
	return rune('A' - 1 + i)
}

func (ec ExcelColumn) Int() int {
	return int(ec)
}
