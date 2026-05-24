package schema

import (
	"testing"
)

func TestCanonicalHashOrderIndependent(t *testing.T) {
	a := []BQField{
		{Name: "b", Type: "STRING", Mode: "NULLABLE"},
		{Name: "a", Type: "INTEGER", Mode: "REQUIRED"},
		{Name: "c", Type: "FLOAT", Mode: "REPEATED"},
	}
	b := []BQField{
		{Name: "c", Type: "FLOAT", Mode: "REPEATED"},
		{Name: "a", Type: "INTEGER", Mode: "REQUIRED"},
		{Name: "b", Type: "STRING", Mode: "NULLABLE"},
	}

	h1 := CanonicalHash(a)
	h2 := CanonicalHash(b)
	if h1 != h2 {
		t.Fatalf("hashes differ: %s vs %s", h1, h2)
	}
}

func TestCanonicalHashOmitsDescription(t *testing.T) {
	a := []BQField{
		{Name: "x", Type: "STRING", Mode: "NULLABLE", Description: "foo"},
	}
	b := []BQField{
		{Name: "x", Type: "STRING", Mode: "NULLABLE", Description: "bar"},
	}
	h1 := CanonicalHash(a)
	h2 := CanonicalHash(b)
	if h1 != h2 {
		t.Fatalf("description should be omitted: %s vs %s", h1, h2)
	}
}

func TestCanonicalHashNestedSorted(t *testing.T) {
	a := []BQField{
		{
			Name: "parent",
			Type: "RECORD",
			Mode: "NULLABLE",
			Fields: []BQField{
				{Name: "z", Type: "STRING", Mode: "NULLABLE"},
				{Name: "a", Type: "INTEGER", Mode: "REQUIRED"},
			},
		},
	}
	b := []BQField{
		{
			Name: "parent",
			Type: "RECORD",
			Mode: "NULLABLE",
			Fields: []BQField{
				{Name: "a", Type: "INTEGER", Mode: "REQUIRED"},
				{Name: "z", Type: "STRING", Mode: "NULLABLE"},
			},
		},
	}
	h1 := CanonicalHash(a)
	h2 := CanonicalHash(b)
	if h1 != h2 {
		t.Fatalf("nested hashes differ: %s vs %s", h1, h2)
	}
}

func TestDiffIdenticalSchemasNONE(t *testing.T) {
	fields := []BQField{
		{Name: "id", Type: "INTEGER", Mode: "REQUIRED"},
		{Name: "name", Type: "STRING", Mode: "NULLABLE"},
	}
	diff := Diff(fields, fields)
	if diff.ChangeType != NONE {
		t.Fatalf("expected NONE, got %s", diff.ChangeType)
	}
	if len(diff.Changes) != 0 {
		t.Fatalf("expected 0 changes, got %d", len(diff.Changes))
	}
	if diff.NewHash != diff.OldHash {
		t.Fatal("hashes should match for identical schemas")
	}
}

func TestDiffAdditiveNullableField(t *testing.T) {
	oldFields := []BQField{
		{Name: "id", Type: "INTEGER", Mode: "REQUIRED"},
	}
	newFields := []BQField{
		{Name: "id", Type: "INTEGER", Mode: "REQUIRED"},
		{Name: "email", Type: "STRING", Mode: "NULLABLE"},
	}
	diff := Diff(oldFields, newFields)
	if diff.ChangeType != ADDITIVE {
		t.Fatalf("expected ADDITIVE, got %s", diff.ChangeType)
	}
	if !diff.IsAdditive() {
		t.Fatal("IsAdditive should return true")
	}
	if len(diff.Changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(diff.Changes))
	}
	if diff.Changes[0].Type != FieldADD {
		t.Fatalf("expected ADD change, got %s", diff.Changes[0].Type)
	}
}

func TestDiffAdditiveRepeatedField(t *testing.T) {
	oldFields := []BQField{
		{Name: "id", Type: "INTEGER", Mode: "REQUIRED"},
	}
	newFields := []BQField{
		{Name: "id", Type: "INTEGER", Mode: "REQUIRED"},
		{Name: "tags", Type: "STRING", Mode: "REPEATED"},
	}
	diff := Diff(oldFields, newFields)
	if diff.ChangeType != ADDITIVE {
		t.Fatalf("expected ADDITIVE, got %s", diff.ChangeType)
	}
}

func TestDiffDropFieldBREAKING(t *testing.T) {
	oldFields := []BQField{
		{Name: "id", Type: "INTEGER", Mode: "REQUIRED"},
		{Name: "name", Type: "STRING", Mode: "NULLABLE"},
	}
	newFields := []BQField{
		{Name: "id", Type: "INTEGER", Mode: "REQUIRED"},
	}
	diff := Diff(oldFields, newFields)
	if diff.ChangeType != BREAKING {
		t.Fatalf("expected BREAKING, got %s", diff.ChangeType)
	}
	if len(diff.Changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(diff.Changes))
	}
	if diff.Changes[0].Type != FieldDROP {
		t.Fatalf("expected DROP change, got %s", diff.Changes[0].Type)
	}
}

func TestDiffTypeChangeBREAKING(t *testing.T) {
	oldFields := []BQField{
		{Name: "id", Type: "INTEGER", Mode: "REQUIRED"},
	}
	newFields := []BQField{
		{Name: "id", Type: "STRING", Mode: "REQUIRED"},
	}
	diff := Diff(oldFields, newFields)
	if diff.ChangeType != BREAKING {
		t.Fatalf("expected BREAKING, got %s", diff.ChangeType)
	}
	if len(diff.Changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(diff.Changes))
	}
	if diff.Changes[0].Type != FieldTypeChange {
		t.Fatalf("expected TYPE_CHANGE, got %s", diff.Changes[0].Type)
	}
}

func TestDiffAdditiveRequiredFieldBREAKING(t *testing.T) {
	oldFields := []BQField{
		{Name: "id", Type: "INTEGER", Mode: "REQUIRED"},
	}
	newFields := []BQField{
		{Name: "id", Type: "INTEGER", Mode: "REQUIRED"},
		{Name: "email", Type: "STRING", Mode: "REQUIRED"},
	}
	diff := Diff(oldFields, newFields)
	if diff.ChangeType != BREAKING {
		t.Fatalf("expected BREAKING for REQUIRED new field, got %s", diff.ChangeType)
	}
}

func TestDiffNestedAdditive(t *testing.T) {
	oldFields := []BQField{
		{
			Name: "address",
			Type: "RECORD",
			Mode: "NULLABLE",
			Fields: []BQField{
				{Name: "city", Type: "STRING", Mode: "NULLABLE"},
			},
		},
	}
	newFields := []BQField{
		{
			Name: "address",
			Type: "RECORD",
			Mode: "NULLABLE",
			Fields: []BQField{
				{Name: "city", Type: "STRING", Mode: "NULLABLE"},
				{Name: "zip", Type: "STRING", Mode: "NULLABLE"},
			},
		},
	}
	diff := Diff(oldFields, newFields)
	if diff.ChangeType != ADDITIVE {
		t.Fatalf("expected ADDITIVE for nested nullable field, got %s", diff.ChangeType)
	}
}

func TestDiffNestedDropBREAKING(t *testing.T) {
	oldFields := []BQField{
		{
			Name: "address",
			Type: "RECORD",
			Mode: "NULLABLE",
			Fields: []BQField{
				{Name: "city", Type: "STRING", Mode: "NULLABLE"},
				{Name: "zip", Type: "STRING", Mode: "NULLABLE"},
			},
		},
	}
	newFields := []BQField{
		{
			Name: "address",
			Type: "RECORD",
			Mode: "NULLABLE",
			Fields: []BQField{
				{Name: "city", Type: "STRING", Mode: "NULLABLE"},
			},
		},
	}
	diff := Diff(oldFields, newFields)
	if diff.ChangeType != BREAKING {
		t.Fatalf("expected BREAKING for nested drop, got %s", diff.ChangeType)
	}
}

func TestDiffNoChangesIsAdditiveFalse(t *testing.T) {
	fields := []BQField{
		{Name: "a", Type: "STRING", Mode: "NULLABLE"},
	}
	diff := Diff(fields, fields)
	if diff.IsAdditive() {
		t.Fatal("IsAdditive should return false for NONE")
	}
}

func TestDiffOrderIndependent(t *testing.T) {
	oldFields := []BQField{
		{Name: "b", Type: "STRING", Mode: "NULLABLE"},
		{Name: "a", Type: "INTEGER", Mode: "REQUIRED"},
	}
	newFields := []BQField{
		{Name: "a", Type: "INTEGER", Mode: "REQUIRED"},
		{Name: "b", Type: "STRING", Mode: "NULLABLE"},
		{Name: "c", Type: "FLOAT", Mode: "NULLABLE"},
	}
	diff := Diff(oldFields, newFields)
	if diff.ChangeType != ADDITIVE {
		t.Fatalf("expected ADDITIVE, got %s", diff.ChangeType)
	}
}

func TestFieldChangePath(t *testing.T) {
	oldFields := []BQField{
		{
			Name: "address",
			Type: "RECORD",
			Mode: "NULLABLE",
			Fields: []BQField{
				{Name: "city", Type: "STRING", Mode: "NULLABLE"},
			},
		},
	}
	newFields := []BQField{
		{
			Name: "address",
			Type: "RECORD",
			Mode: "NULLABLE",
			Fields: []BQField{
				{Name: "city", Type: "STRING", Mode: "NULLABLE"},
				{Name: "zip", Type: "STRING", Mode: "NULLABLE"},
			},
		},
	}
	diff := Diff(oldFields, newFields)
	if len(diff.Changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(diff.Changes))
	}
	if diff.Changes[0].Path != "address.zip" {
		t.Fatalf("expected path 'address.zip', got '%s'", diff.Changes[0].Path)
	}
}

func TestBQFieldModeDefaultsNullable(t *testing.T) {
	oldFields := []BQField{
		{Name: "id", Type: "INTEGER", Mode: "REQUIRED"},
	}
	newFields := []BQField{
		{Name: "id", Type: "INTEGER", Mode: "REQUIRED"},
		{Name: "email", Type: "STRING"},
	}
	diff := Diff(oldFields, newFields)
	if diff.ChangeType != ADDITIVE {
		t.Fatalf("expected ADDITIVE when mode is empty (defaults to NULLABLE), got %s", diff.ChangeType)
	}
}
