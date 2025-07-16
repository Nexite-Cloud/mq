package codec

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
)

type Decoder[T any] func(data []byte) (T, error)

func JSONDecoder[T any](data []byte) (T, error) {
	var v T
	err := json.Unmarshal(data, &v)
	return v, err
}

func GOBDecoder[T any](data []byte) (T, error) {
	var res T
	if err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(&res); err != nil {
		return res, err
	}
	return res, nil
}
