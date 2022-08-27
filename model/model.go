package model

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/civil"
)

type StockData struct {
	SC        int        `bigquery:"sc"`
	Name      string     `bigquery:"name"`
	Market    string     `bigquery:"market"`
	Category  string     `bigquery:"category"`
	Timestamp civil.Date `bigquery:"timestamp"`
	Stock     int        `bigquery:"stock"`
}

func ParseCSV(filename string) ([]StockData, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file; %s; %w", filename, err)
	}
	defer f.Close()

	results := []StockData{}

	r := csv.NewReader(f)
	r.Read() // skip header
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("failed while reading; %w", err)
		}
		stockData, err := stockDataFromString(record)
		if err != nil {
			return nil, err
		}
		results = append(results, stockData)
	}

	return results, nil
}

func stockDataFromString(record []string) (StockData, error) {
	sc, err := strconv.Atoi(record[0])
	if err != nil {
		return StockData{}, fmt.Errorf("failed to parse SC field; %s; %w", record[0], err)
	}
	name := record[1]
	market := record[2]
	category := record[3]
	dt, err := time.Parse("20060102", record[4])
	if err != nil {
		return StockData{}, fmt.Errorf("failed to parse Timestamp field; %s; %w", record[4], err)
	}
	timestamp := civil.Date{Year: dt.Year(), Month: dt.Month(), Day: dt.Day()}
	stock := 0
	stock, _ = strconv.Atoi(record[5]) // ignore error
	return StockData{SC: sc, Name: name, Market: market, Category: category, Timestamp: timestamp, Stock: stock}, nil
}
