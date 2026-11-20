// Package normalize converts the mixed upstream response formats into the
// canonical domain representation. A single synchronization round may contain
// both formats, and every record is normalized independently.
package normalize

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"estuary-collector/internal/domain"
)

// unitFactor converts an upstream unit to mg/L.
//
//	1 g/m^3 == 1 mg/L (numerically equal), but the conversion is expressed
//	explicitly so additional units can be introduced later.
var unitFactor = map[string]*big.Rat{
	"mg/L": big.NewRat(1, 1),
	"g/m3": big.NewRat(1, 1),
	"g/m³": big.NewRat(1, 1),
}

// Record turns one generic JSON object (decoded into a map) into a canonical
// record. The response format is detected per record from its field names.
func Record(raw map[string]any) (domain.CanonicalRecord, error) {
	if len(raw) == 0 {
		return domain.CanonicalRecord{}, errors.New("empty record")
	}
	if _, ok := raw["sample_id"]; ok {
		return newFormat(raw)
	}
	if _, ok := raw["sampleId"]; ok {
		return oldFormat(raw)
	}
	return domain.CanonicalRecord{}, fmt.Errorf("unrecognized record format: %v", keys(raw))
}

func newFormat(raw map[string]any) (domain.CanonicalRecord, error) {
	rev, err := asInt64(raw["rev"])
	if err != nil {
		return domain.CanonicalRecord{}, fmt.Errorf("new-format rev: %w", err)
	}
	unit, _ := raw["unit"].(string)
	mg, err := convertConcentration(raw["concentration"], unit)
	if err != nil {
		return domain.CanonicalRecord{}, err
	}
	status, _ := raw["status"].(string)
	rec := domain.CanonicalRecord{
		Institution: asString(raw["institution"]),
		SampleID:    asString(raw["sample_id"]),
		Revision:    rev,
		SampledOn:   asString(raw["sampled_on"]),
		Location:    asString(raw["site"]),
		Material:    asString(raw["substance"]),
		MgPerL:      mg,
		Unit:        "mg/L",
		PublishedAt: asString(raw["published_at"]),
		Withdrawn:   status == "withdrawn",
	}
	return rec, validate(rec)
}

func oldFormat(raw map[string]any) (domain.CanonicalRecord, error) {
	rev, err := asInt64(raw["revisionId"])
	if err != nil {
		return domain.CanonicalRecord{}, fmt.Errorf("old-format revisionId: %w", err)
	}
	unit, _ := raw["unit"].(string)
	mg, err := convertConcentration(raw["value"], unit)
	if err != nil {
		return domain.CanonicalRecord{}, err
	}
	state, _ := raw["state"].(string)
	rec := domain.CanonicalRecord{
		Institution: asString(raw["institution"]),
		SampleID:    asString(raw["sampleId"]),
		Revision:    rev,
		SampledOn:   asString(raw["samplingDate"]),
		Location:    asString(raw["stationName"]),
		Material:    asString(raw["analyte"]),
		MgPerL:      mg,
		Unit:        "mg/L",
		PublishedAt: asString(raw["reportTime"]),
		Withdrawn:   state == "D",
	}
	return rec, validate(rec)
}

func validate(r domain.CanonicalRecord) error {
	switch {
	case r.Institution == "":
		return errors.New("missing institution")
	case r.SampleID == "":
		return errors.New("missing sample id")
	case r.SampledOn == "":
		return errors.New("missing sampled_on")
	case r.Revision <= 0:
		return fmt.Errorf("invalid revision %d", r.Revision)
	}
	return nil
}

func convertConcentration(v any, unit string) (string, error) {
	factor, ok := unitFactor[unit]
	if !ok {
		return "", fmt.Errorf("unsupported concentration unit %q", unit)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("concentration must be a decimal string, got %T", v)
	}
	rat, ok := new(big.Rat).SetString(strings.TrimSpace(s))
	if !ok {
		return "", fmt.Errorf("invalid concentration %q", s)
	}
	rat.Mul(rat, factor)
	f := new(big.Float).SetPrec(64).SetRat(rat)
	return f.Text('f', -1), nil
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func asInt64(v any) (int64, error) {
	switch t := v.(type) {
	case float64:
		if t != float64(int64(t)) {
			return 0, fmt.Errorf("non-integer revision %v", t)
		}
		return int64(t), nil
	case json.Number:
		return t.Int64()
	case string:
		var n int64
		_, err := fmt.Sscan(t, &n)
		return n, err
	case int64:
		return t, nil
	case int:
		return int64(t), nil
	default:
		return 0, fmt.Errorf("cannot read revision from %T", v)
	}
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
