package dynamodb

import (
	"context"
	awsclient "cost-optimisation/src/aws"
	"cost-optimisation/src/shared/constants"
	"cost-optimisation/src/storage"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

const (
	ROOT               = "/Users/c-andrew.mironov/Work/cost-optimisation/"
	TABLES_PATH        = ROOT + "data/tables.json"
	COST_ANALYSIS_PATH = ROOT + "data/cost_analysis.json"
	TIME_FRAME_DAYS    = 14
)

func AnalyzeDynamdoDB() {

	ctx := context.Background()
	client := awsclient.NewAWSClient(awsclient.AWSClientOpts{
		Region:        constants.US_WEST_2,
		TimeFrameDays: 14,
	})

	fileWriter := storage.NewFileWriter(TABLES_PATH)
	err := fileWriter.Start()
	if err != nil {
		log.Fatalf("Error starting file writer: %v", err)
	}

	dbTables, err := client.GetDynamoDbTables(ctx)
	if err != nil {
		log.Fatalf("Error getting DynamoDB tables: %v", err)
	}

	wg := sync.WaitGroup{}
	for _, tbname := range dbTables {
		wg.Go(func() {
			data := client.ProcessTable(ctx, TIME_FRAME_DAYS, tbname)
			fileWriter.Append(data)
		})
	}

	wg.Wait()
	fileWriter.Close()

	time.Sleep(2 * time.Second) // wait for writes to finish

	err = summ()
	if err != nil {
		log.Fatalf("Error calculating summary: %v", err)
	}
	OptimiseAnalyse(TABLES_PATH, COST_ANALYSIS_PATH)
}

func summ() error {
	fmt.Println("Calculating total cost")
	data, err := os.ReadFile(TABLES_PATH)
	if err != nil {
		fmt.Println(err)
		return err
	}

	var tables []awsclient.TableInfo
	if err := json.Unmarshal(data, &tables); err != nil {
		fmt.Println("JSON parse error:", err)
		return err
	}

	sum := 0.0
	for _, table := range tables {
		sum += parseCost(table.EstimatedCost)
	}

	sortTables(tables)
	fmt.Println("Total cost:", sum)
	return nil
}

func sortTables(tables []awsclient.TableInfo) {
	fmt.Println("Sorting tables")
	for i := 0; i < len(tables)-1; i++ {
		for j := 0; j < len(tables)-i-1; j++ {
			if parseCost(tables[j].EstimatedCost) < parseCost(tables[j+1].EstimatedCost) {
				tables[j], tables[j+1] = tables[j+1], tables[j]
			}
		}
	}

	bytes, err := json.MarshalIndent(tables, "", "  ")
	if err != nil {
		fmt.Println("JSON marshal error:", err)
		return
	}
	os.WriteFile(TABLES_PATH, bytes, 0644)

}

func parseCost(costStr string) float64 {
	var cost float64
	_, err := fmt.Sscanf(costStr, "$%f", &cost)
	if err != nil {
		fmt.Println("Error parsing cost:", err)
		return 0.0
	}
	return cost
}
