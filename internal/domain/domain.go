// Package domain defines the canonical records, revisions and change events
// produced by the collector. All three upstream institutions are normalized
// into these types before anything is written downstream.
package domain

// CanonicalRecord is the unified representation of one suspended-solids
// sampling record. Concentration is always expressed in milligrams per
// litre (mg/L), regardless of the unit used by the source.
type CanonicalRecord struct {
	Institution string `json:"institution"`
	SampleID    string `json:"sample_id"`
	Revision    int64  `json:"revision"`
	SampledOn   string `json:"sampled_on"` // archive day, format YYYY-MM-DD
	Location    string `json:"location"`
	Material    string `json:"material"`
	MgPerL      string `json:"mg_per_l"` // decimal number, kept as string to avoid float drift
	Unit        string `json:"unit"`     // always "mg/L"
	PublishedAt string `json:"published_at"`
	Withdrawn   bool   `json:"withdrawn"`
}

// Key returns the archive key of a record. A reschedule moves a record to a
// different archive day, so the sampled day participates in the key.
func (r CanonicalRecord) Key() string {
	return r.Institution + "|" + r.SampleID + "|" + r.SampledOn
}

// Op is the kind of change published to a downstream batch.
type Op string

const (
	OpUpsert   Op = "upsert"
	OpWithdraw Op = "withdraw"
)

// Change is one entry in a published batch. A reschedule is represented by
// two changes emitted in the same batch: a withdrawal of the old archive day
// followed by an upsert under the new day.
type Change struct {
	Seq         int64            `json:"seq"`
	Op          Op               `json:"op"`
	Institution string           `json:"institution"`
	SampleID    string           `json:"sample_id"`
	SampledOn   string           `json:"sampled_on"`
	Revision    int64            `json:"revision"`
	Record      *CanonicalRecord `json:"record,omitempty"` // present on upsert
}

// Envelope is one JSON Lines row on disk. Every row belongs to a batch and
// carries a globally monotonic sequence number, which is what downstream
// readers use as their saved read position.
type Envelope struct {
	Seq       int64    `json:"seq"`
	BatchID   string   `json:"batch_id"`
	ChangedAt string   `json:"changed_at"`
	Changes   []Change `json:"changes"`
}
