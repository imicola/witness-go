package fixture

import (
	"encoding/json"
	"fmt"
	"io"
	"witness-go/internal/scene"
)

func Read(r io.Reader) (scene.Fixture, error) {
	var f scene.Fixture
	d := json.NewDecoder(r)
	d.DisallowUnknownFields()
	if err := d.Decode(&f); err != nil {
		return f, fmt.Errorf("decode fixture: %w", err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return f, fmt.Errorf("expected one JSON document")
	}
	if f.Provenance == "" {
		return f, fmt.Errorf("missing provenance")
	}
	return f, nil
}
