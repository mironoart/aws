package dynamodb

import (
	awsclient "cost-optimisation/src/aws"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
)

func OptimiseAnalyse(dataPath string, outputPath string) {
	fmt.Println("Start optimisation analysis")
	data, err := os.ReadFile(dataPath)
	if err != nil {
		panic(err)
	}

	var tables []awsclient.TableInfo
	if err := json.Unmarshal(data, &tables); err != nil {
		panic(err)
	}

	savingsSumm := 0.0

	for i, t := range tables {
		if t.BillingMode == "PROVISIONED" {
			analyzeProvisionedTable(&tables[i])
			savingsSumm += tables[i].PotentialSavings
		} else {
			continue
		}
	}

	sort.Slice(tables, func(i, j int) bool {
		return tables[i].PotentialSavings > tables[j].PotentialSavings
	})

	out, _ := json.MarshalIndent(tables, "", "  ")
	if err := os.WriteFile(outputPath, out, 0644); err != nil {
		panic(err)
	}

	WriteStructsToCSV(tables, outputPath+".csv")
	fmt.Println("\n✅ Saved results to analysis.json")
	print("TOTAL POTENTIAL SAVINGS: $", int(savingsSumm), "\n")
}

func analyzeProvisionedTable(t *awsclient.TableInfo) {
	const (
		rcuPrice float64 = 0.00013
		wcuPrice float64 = 0.00065
		hours14d float64 = 24 * 14
	)

	currentCost := (float64(t.ReadCapacityUnits)*rcuPrice + float64(t.WriteCapacityUnits)*wcuPrice) * hours14d
	actualCost := (t.AvgConsumedRead*rcuPrice + t.AvgConsumedWrite*wcuPrice) * hours14d
	utilization := ((t.AvgConsumedRead/float64(t.ReadCapacityUnits) + t.AvgConsumedWrite/float64(t.WriteCapacityUnits)) / 2) * 100

	if currentCost < 0.0001 {
		currentCost = 0.0
	}
	if actualCost < 0.0001 {
		actualCost = 0.0
	}

	potentialSavings := currentCost - actualCost
	if potentialSavings < 0 {
		potentialSavings = 0
	}

	percent := 0.0
	if currentCost > 0 {
		percent = 100 * potentialSavings / currentCost
	}

	rec := ""
	needOptimisation := false
	if utilization < 50 {
		fmt.Printf("potentialSavings: $%.2f\n", potentialSavings)
		rec = "⚠️ Consider switching to PAY_PER_REQUEST (utilization too low)"
		needOptimisation = true
	} else {
		rec = "✅ OK to stay PROVISIONED"
	}

	t.UtilizationPct = utilization
	t.CurrentCost = round(currentCost, 2)
	t.ActualCost = round(actualCost, 2)
	t.PotentialSavings = round(potentialSavings, 2)
	t.PotentialSavingsP = round(percent, 1)
	t.Recommendation = rec
	t.NeedOptimisation = needOptimisation

}

func round(val float64, precision int) float64 {
	format := fmt.Sprintf("%%.%df", precision)
	str := fmt.Sprintf(format, val)
	var out float64
	fmt.Sscanf(str, "%f", &out)
	return out
}

func WriteStructsToCSV(data any, filePath string) error {
	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("expected slice, got %T", data)
	}

	if v.Len() == 0 {
		return fmt.Errorf("empty slice provided")
	}

	elemType := v.Index(0).Type()

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// --- HEADER ---
	var headers []string
	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)
		tag := field.Tag.Get("json")
		if tag == "" {
			headers = append(headers, field.Name)
		} else {
			headers = append(headers, tag)
		}
	}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// --- ROWS ---
	for i := 0; i < v.Len(); i++ {
		rowVal := v.Index(i)
		var record []string
		for j := 0; j < rowVal.NumField(); j++ {
			field := rowVal.Field(j).Interface()
			switch val := field.(type) {
			case string:
				record = append(record, val)
			case *string:
				if val != nil {
					record = append(record, *val)
				} else {
					record = append(record, "")
				}
			case int64:
				record = append(record, strconv.FormatInt(val, 10))
			case float64:
				record = append(record, strconv.FormatFloat(val, 'f', 2, 64))
			case bool:
				record = append(record, strconv.FormatBool(val))
			default:
				record = append(record, fmt.Sprintf("%v", val))
			}
		}

		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
	}

	fmt.Printf("✅ CSV file written successfully: %s\n", filePath)
	return nil
}
