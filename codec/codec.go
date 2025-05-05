package codec

type Codec[T any] interface {
	Encode(data any) ([]byte, error)
	Decode(data []byte) (T, error)
}

type codec[T any] struct {
	encoder Encoder
	decoder Decoder[T]
}

func (c *codec[T]) Encode(data any) ([]byte, error) {
	return c.encoder(data)
}

func (c *codec[T]) Decode(data []byte) (T, error) {
	return c.decoder(data)
}

func JSON[T any]() Codec[T] {
	return &codec[T]{
		encoder: JSONEncoder,
		decoder: JSONDecoder[T],
	}
}

func GOB[T any]() Codec[T] {
	return &codec[T]{
		encoder: GOBEncoder,
		decoder: GOBDecoder[T],
	}
}
