package schema

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"sort"
)

type BQField struct {
	Name        string    `json:"name"`
	Type        string    `json:"type"`
	Mode        string    `json:"mode,omitempty"`
	Description string    `json:"description,omitempty"`
	Fields      []BQField `json:"fields,omitempty"`
}

type ChangeType int

const (
	NONE ChangeType = iota
	ADDITIVE
	BREAKING
)

func (c ChangeType) String() string {
	switch c {
	case NONE:
		return "NONE"
	case ADDITIVE:
		return "ADDITIVE"
	case BREAKING:
		return "BREAKING"
	default:
		return "UNKNOWN"
	}
}

type FieldChangeType string

const (
	FieldADD       FieldChangeType = "ADD"
	FieldDROP      FieldChangeType = "DROP"
	FieldTypeChange FieldChangeType = "TYPE_CHANGE"
)

type FieldChange struct {
	Type   FieldChangeType
	Path   string
	Before *BQField
	After  *BQField
}

type SchemaDiff struct {
	ChangeType ChangeType
	Changes    []FieldChange
	NewHash    string
	OldHash    string
}

func (d *SchemaDiff) IsAdditive() bool {
	return d.ChangeType == ADDITIVE
}

func CanonicalHash(fields []BQField) string {
	sorted := sortFieldsCopy(fields)
	h := sha256.New()
	h.Write(canonicalBytes(sorted))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func canonicalBytes(fields []BQField) []byte {
	var buf bytes.Buffer
	for i, f := range fields {
		if i > 0 {
			buf.WriteByte(0)
		}
		buf.WriteString(f.Name)
		buf.WriteByte(0)
		buf.WriteString(f.Type)
		buf.WriteByte(0)
		buf.WriteString(f.Mode)
		if len(f.Fields) > 0 {
			buf.WriteByte(0)
			buf.WriteByte('{')
			buf.Write(canonicalBytes(f.Fields))
			buf.WriteByte('}')
		}
	}
	return buf.Bytes()
}

func sortFieldsCopy(fields []BQField) []BQField {
	if fields == nil {
		return nil
	}
	out := make([]BQField, len(fields))
	copy(out, fields)
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	for i := range out {
		if len(out[i].Fields) > 0 {
			out[i].Fields = sortFieldsCopy(out[i].Fields)
		}
	}
	return out
}

type diffCtx struct {
	oldMap map[string]BQField
	newMap map[string]BQField
}

func Diff(oldFields, newFields []BQField) *SchemaDiff {
	oldSorted := sortFieldsCopy(oldFields)
	newSorted := sortFieldsCopy(newFields)

	oldHash := CanonicalHash(oldSorted)
	newHash := CanonicalHash(newSorted)

	if oldHash == newHash {
		return &SchemaDiff{
			ChangeType: NONE,
			NewHash:    newHash,
			OldHash:    oldHash,
		}
	}

	changes := diffFields(oldSorted, newSorted, "")

	return &SchemaDiff{
		ChangeType: classifyChanges(changes),
		Changes:    changes,
		NewHash:    newHash,
		OldHash:    oldHash,
	}
}

func diffFields(oldFields, newFields []BQField, prefix string) []FieldChange {
	oldMap := make(map[string]BQField, len(oldFields))
	for _, f := range oldFields {
		oldMap[f.Name] = f
	}
	newMap := make(map[string]BQField, len(newFields))
	for _, f := range newFields {
		newMap[f.Name] = f
	}

	allNames := make(map[string]bool)
	for n := range oldMap {
		allNames[n] = true
	}
	for n := range newMap {
		allNames[n] = true
	}
	sortedNames := make([]string, 0, len(allNames))
	for n := range allNames {
		sortedNames = append(sortedNames, n)
	}
	sort.Strings(sortedNames)

	var changes []FieldChange
	for _, name := range sortedNames {
		oldF, inOld := oldMap[name]
		newF, inNew := newMap[name]

		path := name
		if prefix != "" {
			path = prefix + "." + name
		}

		switch {
		case inNew && !inOld:
			f := newF
			changes = append(changes, FieldChange{Type: FieldADD, Path: path, After: &f})
		case inOld && !inNew:
			f := oldF
			changes = append(changes, FieldChange{Type: FieldDROP, Path: path, Before: &f})
		default:
			if oldF.Type != newF.Type {
				changes = append(changes, FieldChange{
					Type:   FieldTypeChange,
					Path:   path,
					Before: &oldF,
					After:  &newF,
				})
			} else {
				changes = append(changes, diffFields(oldF.Fields, newF.Fields, path)...)
			}
		}
	}

	return changes
}

func ClassifyChange(changes []FieldChange) ChangeType {
	return classifyChanges(changes)
}

func isTypeWidening(before, after *BQField) bool {
	widening := map[string][]string{
		"INT64":    {"FLOAT64", "NUMERIC", "BIGNUMERIC"},
		"FLOAT64":  {"NUMERIC", "BIGNUMERIC"},
		"NUMERIC":  {"BIGNUMERIC"},
		"DATE":     {"DATETIME", "TIMESTAMP"},
		"DATETIME": {"TIMESTAMP"},
	}
	valid, ok := widening[before.Type]
	if !ok {
		return false
	}
	for _, v := range valid {
		if after.Type == v {
			return true
		}
	}
	return false
}

func classifyChanges(changes []FieldChange) ChangeType {
	if len(changes) == 0 {
		return NONE
	}
	for _, c := range changes {
		switch c.Type {
		case FieldDROP:
			return BREAKING
		case FieldTypeChange:
			if c.Before != nil && c.After != nil && isTypeWidening(c.Before, c.After) {
				continue
			}
			return BREAKING
		case FieldADD:
			if c.After == nil {
				return BREAKING
			}
			mode := c.After.Mode
			if mode == "" {
				mode = "NULLABLE"
			}
			if mode != "NULLABLE" && mode != "REPEATED" {
				return BREAKING
			}
		}
	}
	return ADDITIVE
}
