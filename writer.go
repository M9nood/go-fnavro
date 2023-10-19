package fnavro

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/hamba/avro/v2/ocf"
)

type FnAvroWriter struct {
	encoder *ocf.Encoder
	writer  io.WriteCloser
	ctx     context.Context
}

func (w *FnAvroWriter) Close() error {
	if err := w.encoder.Flush(); err != nil {
		return fmt.Errorf("encoder can not flush: %w", err)
	}
	if err := w.writer.Close(); err != nil {
		return fmt.Errorf("writer close error: %w", err)
	}
	return nil
}

func (w *FnAvroWriter) Append(data interface{}) error {
	return w.encoder.Encode(data)
}

func (w *FnAvroWriter) MapAndAppend(source, target any) error {
	jsonStr, err := json.Marshal(source)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(jsonStr, &target); err != nil {
		return err
	}
	return w.Append(target)
}
