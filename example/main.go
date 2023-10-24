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
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/api/option"
)

type Nav struct {
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
	os.Setenv("FNAVRO_EXPORT_BUCKET", "gs://scontent-qa.finnomena.com")
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "./service-account.json")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gcs, _ := storage.NewClient(ctx, option.WithCredentialsFile(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")))
	bucket := os.Getenv("FNAVRO_EXPORT_BUCKET")
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
		fmt.Printf("fnavro client GetSchema failed, %v", err)
	}

	now := time.Now()

	// append
	sampleValue, _ := new(big.Rat).SetString("19")
	sampleAmout, _ := new(big.Rat).SetString("140999")

	sampleRecords1 := []AvroNav{
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
	distFileName1 := fmt.Sprintf("%s_%s_000000", entityName, now.Format("2006-01-02"))
	outputDir1 := fmt.Sprintf("%s/%s/%s/%s", bucket, namespace, entityName, now.Format("2006/01/02"))
	avroWriter1, _ := fnavroClient.NewAvroWriter(schema, outputDir1, distFileName1, 1)
	for i := 0; i < len(sampleRecords1); i++ {
		avro := AvroNav{}
		if err := avroWriter1.MapAndAppend(sampleRecords1[i], &avro); err != nil {
			fmt.Printf("avro append data error: %s", err.Error())
			return
		}
	}

	if err := avroWriter1.Close(); err != nil {
		log.Panicf("Avro close process error: %s\n", err.Error())
		return
	}

	// Map to other struct and append

	sampleRecords2 := []Nav{
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

	distFileName2 := fmt.Sprintf("map%s_%s_000000", entityName, now.Format("2006-01-02"))
	outputDir2 := fmt.Sprintf("%s/%s/%s/%s", bucket, namespace, entityName, now.Format("2006/01/02"))
	avroWriter2, _ := fnavroClient.NewAvroWriter(schema, outputDir2, distFileName2, 1)
	for i := 0; i < len(sampleRecords2); i++ {
		avro := AvroNav{}
		if err := avroWriter2.MapAndAppend(sampleRecords2[i], &avro); err != nil {
			fmt.Printf("avro append data error: %s", err.Error())
			return
		}
	}

	if err := avroWriter2.Close(); err != nil {
		log.Panicf("Avro close process error: %s\n", err.Error())
		return
	}

}
