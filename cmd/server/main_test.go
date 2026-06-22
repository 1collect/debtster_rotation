package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/xuri/excelize/v2"
)

func TestToDecimalParsesFormattedAmounts(t *testing.T) {
	tests := map[string]string{
		"1234567.89":             "1234567.89",
		"1 234 567,89":           "1234567.89",
		"1\u00a0234\u00a0567,89": "1234567.89",
		"1.234.567,89":           "1234567.89",
		"1,234,567.89":           "1234567.89",
		"1 234 567":              "1234567",
		"1.234.567":              "1234567",
		"1,234,567":              "1234567",
		"1234567,89 тг":          "1234567.89",
		"-1 234,50":              "-1234.5",
		"99,274":                 "99274",
		"213,389":                "213389",
		"5.6888806E+06":          "5688880.6",
		"5,6888806E+06":          "5688880.6",
		"1.0665008e+007":         "10665008",
	}

	for input, want := range tests {
		got := toDecimal(input).String()
		if got != want {
			t.Fatalf("toDecimal(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestCollectFinalLoadsUsesFinalAttachColumn(t *testing.T) {
	workbook := excelize.NewFile()
	sheet := workbook.GetSheetName(0)
	headers := []any{"РП", "Заказчик", "ФИО", "ИИН", "Общая задолженность", "Открепить", "Статус", "Закрепить"}
	for col, value := range headers {
		if err := setCell(workbook, sheet, 1, col+1, value); err != nil {
			t.Fatal(err)
		}
	}
	rows := [][]any{
		{"РП1", "З", "А", "111", "5.6888806E+06", "OLD_LOGIN", "Связь с должником", "NEW_LOGIN"},
		{"РП1", "З", "Б", "222", "1 234 567,89", "OLD_LOGIN", "Оплата по соглашению", "NEW_LOGIN"},
	}
	for rowIndex, row := range rows {
		for col, value := range row {
			if err := setCell(workbook, sheet, rowIndex+2, col+1, value); err != nil {
				t.Fatal(err)
			}
		}
	}

	header, err := readHeader(workbook, sheet)
	if err != nil {
		t.Fatal(err)
	}
	cols, err := findColumns(header)
	if err != nil {
		t.Fatal(err)
	}
	loads, _ := collectFinalLoads(workbook, sheet, cols)
	got := loads[loginKey{rp: "РП1", login: "NEW_LOGIN"}]
	if got == nil {
		t.Fatal("missing final login load")
	}
	if got.count != 2 {
		t.Fatalf("count = %d, want 2", got.count)
	}
	if got.amount.String() != "6923448.49" {
		t.Fatalf("amount = %s, want 6923448.49", got.amount.String())
	}
	if got.iinCount != 2 {
		t.Fatalf("iinCount = %d, want 2", got.iinCount)
	}
}

func TestHeaderLikeAttachValueDoesNotBecomeLogin(t *testing.T) {
	rows := [][]string{
		{"РП", "Заказчик", "ФИО", "ИИН", "Общая задолженность", "Открепить", "Статус", "Закрепить"},
		{"РП1", "З", "А", "111", "100", "VALID_LOGIN", "Связь с должником", "ЗАКРЕПИТЬ"},
		{"РП", "Заказчик", "ФИО", "ИИН", "Общая задолженность", "Открепить", "Статус", "Закрепить"},
	}
	header, err := readHeaderFromRows(rows)
	if err != nil {
		t.Fatal(err)
	}
	cols, err := findColumns(header)
	if err != nil {
		t.Fatal(err)
	}

	loginKeys := readLoginKeysFromRows(rows, cols, "attach")
	if len(loginKeys) != 1 {
		t.Fatalf("login key count = %d, want 1: %+v", len(loginKeys), loginKeys)
	}
	if loginKeys[0] != (loginKey{rp: "РП1", login: "VALID_LOGIN"}) {
		t.Fatalf("login key = %+v, want РП1/VALID_LOGIN", loginKeys[0])
	}
}

func TestCrossRPRotationWritesSourceRPAndKeepsRowBalance(t *testing.T) {
	workbook := excelize.NewFile()
	sheet := workbook.GetSheetName(0)
	headers := []any{"РП", "ИИН", "Общая задолженность", "Открепить", "Статус", "Закрепить"}
	for col, value := range headers {
		if err := setCell(workbook, sheet, 1, col+1, value); err != nil {
			t.Fatal(err)
		}
	}
	rows := [][]any{
		{"РП_A", "999", "1000", "LOGIN_A", "Оплата ПВ", ""},
		{"РП_A", "111", "1000", "LOGIN_A", "В работе", ""},
		{"РП_B", "222", "1", "LOGIN_B", "В работе", ""},
	}
	for rowIndex, row := range rows {
		for col, value := range row {
			if err := setCell(workbook, sheet, rowIndex+2, col+1, value); err != nil {
				t.Fatal(err)
			}
		}
	}

	var input bytes.Buffer
	if err := workbook.Write(&input); err != nil {
		t.Fatal(err)
	}

	app := &server{
		hub:  &progressHub{clients: make(map[string]map[chan payload]bool)},
		jobs: make(map[string]*jobResult),
	}
	output, err := redistributeWorkbook(context.Background(), bytes.NewReader(input.Bytes()), "test-cross-rp", app, workbookConfig{
		fixedStatuses: rotationFixedStatuses,
		sourceColumn:  "detach",
		strategy:      "cross_rp",
		processName:   "ротации между РП",
		summaryTitle:  "Итоги ротации между РП",
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := excelize.OpenReader(bytes.NewReader(output))
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()

	resultSheet := result.GetSheetName(0)
	header, err := readHeader(result, resultSheet)
	if err != nil {
		t.Fatal(err)
	}
	cols, err := findColumns(header)
	if err != nil {
		t.Fatal(err)
	}
	if cols.sourceRP == 0 {
		t.Fatal("missing Исходное рп column")
	}

	expensiveRowRP := normalizeRP(getCell(result, resultSheet, 3, cols.rp))
	expensiveRowSourceRP := normalizeRP(getCell(result, resultSheet, 3, cols.sourceRP))
	expensiveRowLogin := normalizeLogin(getCell(result, resultSheet, 3, cols.attach))
	if expensiveRowRP != "РП_A" || expensiveRowSourceRP != "РП_A" || expensiveRowLogin != "LOGIN_A" {
		t.Fatalf("expensive row = rp/source/login %q/%q/%q, want РП_A/РП_A/LOGIN_A", expensiveRowRP, expensiveRowSourceRP, expensiveRowLogin)
	}

	smallRowRP := normalizeRP(getCell(result, resultSheet, 4, cols.rp))
	smallRowSourceRP := normalizeRP(getCell(result, resultSheet, 4, cols.sourceRP))
	smallRowLogin := normalizeLogin(getCell(result, resultSheet, 4, cols.attach))
	if smallRowRP != "РП_B" || smallRowSourceRP != "РП_B" || smallRowLogin != "LOGIN_B" {
		t.Fatalf("small row = rp/source/login %q/%q/%q, want РП_B/РП_B/LOGIN_B", smallRowRP, smallRowSourceRP, smallRowLogin)
	}
}

func TestReplaceSummarySheetWritesAmountWithoutFixedDecimalPlaces(t *testing.T) {
	workbook := excelize.NewFile()
	amount := decimal.RequireFromString("1234.56")
	loads := map[loginKey]*load{
		{rp: "РП1", login: "LOGIN1"}: {
			count:    1,
			amount:   amount,
			iinCount: 1,
		},
	}

	if err := replaceSummarySheet(workbook, loads, 0, 0, "Итоги"); err != nil {
		t.Fatal(err)
	}

	got, err := workbook.GetCellValue("Итоги", "D2")
	if err != nil {
		t.Fatal(err)
	}
	if !toDecimal(got).Equal(amount) {
		t.Fatalf("D2 = %q, want %q", got, amount.String())
	}
	cellType, err := workbook.GetCellType("Итоги", "D2")
	if err != nil {
		t.Fatal(err)
	}
	if cellType != excelize.CellTypeNumber && cellType != excelize.CellTypeUnset {
		t.Fatalf("D2 type = %v, want numeric cell", cellType)
	}
}

func TestReplaceSummarySheetPreservesExactTotal(t *testing.T) {
	workbook := excelize.NewFile()
	loads := map[loginKey]*load{
		{rp: "РП1", login: "LOGIN1"}: {
			count:    1,
			amount:   decimal.RequireFromString("1234567890.67"),
			iinCount: 1,
		},
		{rp: "РП1", login: "LOGIN2"}: {
			count:    1,
			amount:   decimal.RequireFromString("0.33"),
			iinCount: 1,
		},
	}

	if err := replaceSummarySheet(workbook, loads, 0, 0, "Итоги"); err != nil {
		t.Fatal(err)
	}

	rows, err := workbook.GetRows("Итоги")
	if err != nil {
		t.Fatal(err)
	}
	total := decimal.Zero
	for _, row := range rows[1:] {
		if normalizeRP(getRowCell(row, 1)) == "" || normalizeLogin(getRowCell(row, 2)) == "" {
			continue
		}
		total = total.Add(toDecimal(getRowCell(row, 4)))
	}

	want := decimal.RequireFromString("1234567891")
	if !total.Equal(want) {
		t.Fatalf("summary total = %s, want %s", total.String(), want.String())
	}
}
