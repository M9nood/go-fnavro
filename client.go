package fnavro

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"

	"cloud.google.com/go/storage"
	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
)

type FnAvroClient struct {
	ctx           context.Context
	storageType   StorageType
	storageClient *storage.Client
}

type StorageType string

const (
	GoogleStorageType StorageType = "gcs"
	FileStorageType   StorageType = "file"
)

type FnAvroOption func(*FnAvroClient)

func WithGoogleStorageClient(client *storage.Client) FnAvroOption {
	return func(ac *FnAvroClient) {
		ac.storageType = GoogleStorageType
		ac.storageClient = client
	}
}

func WithFileStorageClient(client *storage.Client) FnAvroOption {
	return func(ac *FnAvroClient) {
		ac.storageType = FileStorageType
		ac.storageClient = client
	}
}

func NewFnAvroClient(ctx context.Context, opts ...FnAvroOption) (*FnAvroClient, error) {
	client := &FnAvroClient{ctx: ctx}
	for _, opt := range opts {
		opt(client)
	}
	return client, nil
}

func (ac *FnAvroClient) GetSchema(schemaPath string) (avro.Schema, error) {
	text, err := ac.Read(ac.ctx, schemaPath)
	if err != nil {
		return nil, fmt.Errorf("can not read schema: %w", err)
	}
	schema, err := avro.Parse(text)
	if err != nil {
		return nil, fmt.Errorf("invalid schema: %w", err)
	}
	return schema, nil

}

func (ac *FnAvroClient) NewAvroWriter(schema avro.Schema, dir string, filename string, part int) (*FnAvroWriter, error) {
	var destination string = dir
	partition := fmt.Sprintf("%03d", part-1)

	if part > 1 {
		destination = fmt.Sprintf("%s/%s.%s.avro", destination, filename, partition)
	} else {
		destination = fmt.Sprintf("%s/%s.avro", destination, filename)
	}
	writer, err := ac.GetWriter(ac.ctx, destination)
	if err != nil {
		return nil, fmt.Errorf("can not create writer: %w", err)
	}
	enc, err := ocf.NewEncoder(schema.String(), writer)
	if err != nil {
		return nil, fmt.Errorf("can not create decoder: %w", err)
	}
	return &FnAvroWriter{encoder: enc, writer: writer, ctx: ac.ctx}, nil
}

func (c *FnAvroClient) Read(ctx context.Context, path string) (string, error) {
	switch c.storageType {
	default:
		b, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		return string(b), nil
	case GoogleStorageType:
		obj, err := GetGCSObject(c.storageClient, path)
		if err != nil {
			return "", fmt.Errorf("get gcs object error: %w", err)
		}
		reader, err := obj.NewReader(ctx)
		if err != nil {
			return "", fmt.Errorf("reader error: %w", err)
		}
		defer reader.Close()

		b, err := io.ReadAll(reader)
		if err != nil {
			return "", fmt.Errorf("read all error: %w", err)
		}
		return string(b), nil
	}
}

func (c *FnAvroClient) GetWriter(ctx context.Context, path string) (io.WriteCloser, error) {
	switch c.storageType {
	default:
		return os.Create(path)
	case GoogleStorageType:
		obj, err := GetGCSObject(c.storageClient, path)
		if err != nil {
			return nil, err
		}
		return obj.NewWriter(ctx), nil
	}
}

func GetGCSObject(client *storage.Client, uri string) (*storage.ObjectHandle, error) {
	r, _ := regexp.Compile("gs://(.*?)/(.*)")
	match := r.FindStringSubmatch(uri)
	if len(match) != 3 {
		return nil, fmt.Errorf("'%s' is invalid gcs uri", uri)
	}
	return client.Bucket(match[1]).Object(match[2]), nil
}
