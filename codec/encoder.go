package codec

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
)

type Encoder func(data any) ([]byte, error)

var JSONEncoder Encoder = func(data any) ([]byte, error) {
	return json.Marshal(data)
}

var GOBEncoder Encoder = func(data any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
