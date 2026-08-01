package store

import (
	"reflect"
	"strings"
	"testing"
	"unicode"
)

// A field added to DevshardRecord is written once by the insert and then never again, because the
// update clause names its columns by hand. Nothing else in the package notices: the insert compiles,
// the scan compiles, and only a re-registration quietly keeps the old value. This is the notice.
func TestTheDevshardUpsertCarriesEveryFieldItShould(t *testing.T) {
	// escrow_id is the conflict key, and settlement_pending is left out on purpose so an unrelated
	// upsert cannot clear a queued settlement. Everything else must survive a re-registration.
	deliberatelyNotUpdated := map[string]string{
		"escrow_id":          "the conflict key",
		"settlement_pending": "only SetDevshardSettlementPending moves it",
	}

	recordType := reflect.TypeOf(DevshardRecord{})
	for index := range recordType.NumField() {
		column := columnName(recordType.Field(index).Name)
		if reason, expected := deliberatelyNotUpdated[column]; expected {
			if strings.Contains(upsertDevshardStatement, column+" = excluded.") {
				t.Fatalf("%s is updated after all, but is documented as left alone: %s", column, reason)
			}
			continue
		}
		if !strings.Contains(upsertDevshardStatement, column+" = excluded."+column) {
			t.Fatalf("DevshardRecord.%s maps to column %q, which the upsert inserts but never updates:"+
				" re-registering an escrow would silently keep its old value",
				recordType.Field(index).Name, column)
		}
	}
}

// columnName turns a Go field name into the snake_case column this schema uses. An acronym stays one
// word: EscrowID is escrow_id, not escrow_i_d.
func columnName(field string) string {
	runes := []rune(field)
	var column strings.Builder
	for index, symbol := range runes {
		if unicode.IsUpper(symbol) && index > 0 {
			startsWord := unicode.IsLower(runes[index-1])
			endsAcronym := index+1 < len(runes) && unicode.IsLower(runes[index+1])
			if startsWord || endsAcronym {
				column.WriteByte('_')
			}
		}
		column.WriteRune(unicode.ToLower(symbol))
	}
	return column.String()
}

func TestColumnNameKeepsAcronymsWhole(t *testing.T) {
	for field, want := range map[string]string{
		"EscrowID":          "escrow_id",
		"PrivateKeyEnv":     "private_key_env",
		"SettlementPending": "settlement_pending",
		"Active":            "active",
	} {
		if got := columnName(field); got != want {
			t.Errorf("columnName(%q) = %q, want %q", field, got, want)
		}
	}
}
