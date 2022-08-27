package main

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
	"github.com/hsmtkk/congenial-adventure/model"
	"google.golang.org/api/option"
)

const projectID = "bigquery-training-360621"
const datasetID = "stock"
const tableID = "stocktable"
const credentialFile = "credential.json"

func main() {
	stockData, err := model.ParseCSV("japan-all-stock-prices_20220826.csv")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%v\n", stockData[0])

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(credentialFile))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	up := client.Dataset(datasetID).Table(tableID).Uploader()
	if err := up.Put(ctx, stockData); err != nil {
		log.Fatal(err)
	}
}
