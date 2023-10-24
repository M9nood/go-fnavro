# go-fnavro

Library generate and export avro file


## Usage

### Append

```go

package main

import (
	"context"
	"fmt"
	"fnavro"
	"log"
	"math/big"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

type AvroNav struct {
	MstarId string    `json:"mstar_id" avro:"mstar_id"`
	NavDate time.Time `json:"nav_date" avro:"nav_date"`
	Value   *big.Rat  `json:"value" avro:"value"`
	Amount  *big.Rat  `json:"amount" avro:"amount"`
}

func main() {
	os.Setenv("FNAVRO_EXPORT_BUCKET", "gs://XXXXXX")
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "./service-account.json")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gcs, _ := storage.NewClient(ctx, option.WithCredentialsFile(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")))
	bucket := "sample-bucket"
	namespace := "avrotest/knowledge-hub/fund"
	entityName := "nav"
	schemaFileName := "schema.avsc"

	fnavroClient, err := fnavro.NewFnAvroClient(ctx, fnavro.WithGoogleStorageClient(gcs))
	if err != nil {
		fmt.Printf("fnavro client failed, %v", err)
	}
	schemaPath := fmt.Sprintf("%s/%s/%s/%s", bucket, namespace, entityName, schemaFileName)
	schema, err := fnavroClient.GetSchema(schemaPath)
	if err != nil {
		fmt.Printf("fnavro client get schema failed, %v", err)
	}

	now := time.Now()

	// append
	sampleValue, _ := new(big.Rat).SetString("19")
	sampleAmout, _ := new(big.Rat).SetString("140999")

	sampleRecords := []AvroNav{
		{
			MstarId: "F000000010",
			NavDate: now,
			Value:   sampleValue,
			Amount:  sampleAmout,
		},
		{
			MstarId: "F000000011",
			NavDate: now,
			Value:   sampleValue,
			Amount:  sampleAmout,
		},
	}
	distFileName := fmt.Sprintf("%s_%s_000000", entityName, now.Format("2006-01-02"))
	outputDir := fmt.Sprintf("%s/%s/%s/%s", bucket, namespace, entityName, now.Format("2006/01/02"))
	avroWriter, _ := fnavroClient.NewAvroWriter(schema, outputDir, distFileName1, 1)
	for i := 0; i < len(sampleRecords); i++ {
		if err := avroWriter.Append(sampleRecords[i]); err != nil {
			fmt.Printf("avro append data error: %s", err.Error())
			return
		}
	}

	if err := avroWriter.Close(); err != nil {
		log.Panicf("avro close process error: %s\n", err.Error())
		return
	}

}

```


### MapAndAppend

Map data to other struct (avro) by `json` tag

```go

package main

import (
	"context"
    ...
)

type NavFromDB struct {
	MstarId string             `bson:"mstar_id" json:"mstar_id"`
	NavDate primitive.DateTime `bson:"nav_date" json:"nav_date"`
	Value   decimal.Decimal    `bson:"value" json:"value"`
	Amount  decimal.Decimal    `bson:"amount" json:"amount"`
}

type AvroNav struct {
	MstarId string    `json:"mstar_id" avro:"mstar_id"`
	NavDate time.Time `json:"nav_date" avro:"nav_date"`
	Value   *big.Rat  `json:"value" avro:"value"`
	Amount  *big.Rat  `json:"amount" avro:"amount"`
}

func main() {
    ...
    sampleRecords := []NavFromDB{
		{
			MstarId: "F000000010",
			NavDate: primitive.NewDateTimeFromTime(now),
			Value:   decimal.NewFromFloat(30.4),
			Amount:  decimal.NewFromFloat(12039.4),
		},
		{
			MstarId: "F000000011",
			NavDate: primitive.NewDateTimeFromTime(now),
			Value:   decimal.NewFromFloat(30.4),
			Amount:  decimal.NewFromFloat(12040.4),
		},
	}

	distFileName := fmt.Sprintf("%s_%s_000000", entityName, now.Format("2006-01-02"))
	...

	for i := 0; i < len(sampleRecords); i++ {
		avro := AvroNav{}
		if err := avroWriter2.MapAndAppend(sampleRecords[i], &avro); err != nil {
			fmt.Printf("avro append data error: %s", err.Error())
			return
		}
	}

	if err := avroWriter.Close(); err != nil {
		log.Panicf("Avro close process error: %s\n", err.Error())
		return
	}

}

```