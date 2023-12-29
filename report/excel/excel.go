package excel

import (
	"fmt"
	"log"

	"github.com/xuri/excelize/v2"
)

type ExcelFile struct {
	F                 *excelize.File
	SheetName         string
	StartHeaderColumn int
	StartHeaderRow    int
	StartValueColumn  int
	StartValueRow     int
}

func NewFile(opts ...excelize.Options) *ExcelFile {

	f := excelize.NewFile(opts...)
	defer func() {
		if err := f.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	return &ExcelFile{F: f}
}

func (e *ExcelFile) NewSheet(sheetName string) (index int, err error) {
	e.SheetName = sheetName
	return e.F.NewSheet(sheetName)
}

// startColumn represent where alphabet of column start e.g 1 = A; 2 = B; 3 = C; . . .; 26 = Z
// startRow represent where number of row start e.g 1; 2; 3;
// headersName represent of what you will given header name in excel
func (e *ExcelFile) SetHeader(startColumn int, startRow int, headersName ...string) *ExcelFile {

	e.StartHeaderColumn = startColumn
	e.StartHeaderRow = startRow

	setHeaders := make(map[string]string)
	for idx, val := range headersName {
		setHeaders[string(ToChar(idx+e.StartHeaderColumn))+fmt.Sprint(e.StartHeaderRow)] = val
	}

	for k, v := range setHeaders {
		e.F.SetCellValue(e.SheetName, k, v)
	}

	return e
}

// startColumn represent where alphabet of column start e.g 1 = A; 2 = B; 3 = C; . . .; 26 = Z
// startRow represent where number of row start e.g 1; 2; 3;
// data represent of what data will fill in sheet
func (e *ExcelFile) SetValues(startColumn int, startRow int, data ...any) *ExcelFile {

	if e.StartValueColumn == 0 && e.StartValueRow == 0 {
		e.StartValueColumn = startColumn
		e.StartValueRow = startRow
	}

	for idx, val := range data {
		switch v := val.(type) {
		default:
			e.F.SetCellValue(e.SheetName, string(ToChar(idx+e.StartValueColumn))+fmt.Sprint(e.StartValueRow), v)
		}
	}

	return e
}

func (e *ExcelFile) MergeCellColumn(mergeCell map[string]string) *ExcelFile {

	for k, v := range mergeCell {
		e.F.MergeCell(e.SheetName, k, v)
	}

	return e
}

func (e *ExcelFile) NextRowValue() {
	e.StartValueRow++
}

func (e *ExcelFile) NextColumnValue() {
	e.StartValueColumn++
}

func (e *ExcelFile) WriteToByte() []byte {
	buff, err := e.F.WriteToBuffer()
	if err != nil {
		return nil
	}
	return buff.Bytes()
}
