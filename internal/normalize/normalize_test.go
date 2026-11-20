package normalize

import (
	"testing"

	"estuary-collector/internal/domain"
)

func TestNewAndLegacyFormats(t *testing.T) {
	cases := []struct {
		name string
		raw  map[string]any
		want domain.CanonicalRecord
	}{
		{
			name: "new format mg/L",
			raw: map[string]any{
				"institution": "alpha", "sample_id": "A-1", "rev": float64(3),
				"sampled_on": "2026-11-10", "site": "北滩", "substance": "悬浮物",
				"concentration": "12.5", "unit": "mg/L", "status": "active",
				"published_at": "2026-11-10T08:00:00Z",
			},
			want: domain.CanonicalRecord{Institution: "alpha", SampleID: "A-1", Revision: 3,
				SampledOn: "2026-11-10", Location: "北滩", Material: "悬浮物",
				MgPerL: "12.5", Unit: "mg/L", PublishedAt: "2026-11-10T08:00:00Z"},
		},
		{
			name: "legacy format g/m3 maps unit and withdrawn state",
			raw: map[string]any{
				"institution": "bravo", "sampleId": "B-1", "revisionId": float64(2),
				"samplingDate": "2026-11-11", "stationName": "南港", "analyte": "悬浮物",
				"value": "10.5", "unit": "g/m3", "state": "D",
				"reportTime": "2026-11-12T10:00:00Z",
			},
			want: domain.CanonicalRecord{Institution: "bravo", SampleID: "B-1", Revision: 2,
				SampledOn: "2026-11-11", Location: "南港", Material: "悬浮物",
				MgPerL: "10.5", Unit: "mg/L", PublishedAt: "2026-11-12T10:00:00Z", Withdrawn: true},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Record(tc.raw)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("normalized record mismatch:\n got %+v\nwant %+v", got, tc.want)
			}
		})
	}
}

func TestRejectsUnknownFormatAndUnit(t *testing.T) {
	if _, err := Record(map[string]any{"foo": "bar"}); err == nil {
		t.Fatal("expected unknown-format error")
	}
	raw := map[string]any{
		"institution": "alpha", "sample_id": "A-1", "rev": float64(1),
		"sampled_on": "2026-11-10", "concentration": "1", "unit": "ppm",
	}
	if _, err := Record(raw); err == nil {
		t.Fatal("expected unsupported-unit error")
	}
}
