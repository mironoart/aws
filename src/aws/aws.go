package awsclient

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type TableInfo struct {
	TableName          string  `json:"tableName"`
	BillingMode        string  `json:"billingMode"`
	ItemCount          int64   `json:"itemCount"`
	TableSizeMB        int64   `json:"tableSizeMB"`
	ReadCapacityUnits  int64   `json:"readCapacityUnits"`
	WriteCapacityUnits int64   `json:"writeCapacityUnits"`
	AvgConsumedRead    float64 `json:"avgConsumedRead"`
	AvgConsumedWrite   float64 `json:"avgConsumedWrite"`
	MetricsAvailable   bool    `json:"metricsAvailable"`
	TableArn           *string `json:"tableArn"`
	EstimatedCost      string  `json:"estimatedCost"`
	UtilizationPct     float64 `json:"utilizationPct"`
	CurrentCost        float64 `json:"currentCost"`
	ActualCost         float64 `json:"actualCost"`
	PotentialSavings   float64 `json:"potentialSavings"`
	PotentialSavingsP  float64 `json:"potentialSavingsP"`
	Recommendation     string  `json:"recommendation"`
	NeedOptimisation   bool    `json:"needOptimisation"`
}

type AWSClientOpts struct {
	Region        string
	TimeFrameDays int
}

type AWSClient struct {
	DynamoDB   *dynamodb.Client
	CloudWatch *cloudwatch.Client
}

func NewAWSClient(opts AWSClientOpts) *AWSClient {
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(opts.Region))
	if err != nil {
		log.Fatalf("unable to load SDK config: %v", err)
	}

	return &AWSClient{
		DynamoDB:   dynamodb.NewFromConfig(cfg),
		CloudWatch: cloudwatch.NewFromConfig(cfg),
	}

}

func (c *AWSClient) GetDynamoDbTables(ctx context.Context) ([]string, error) {

	var allTables []string
	var exclusiveStartTableName *string
	listTablesInput := &dynamodb.ListTablesInput{}

	for {
		if exclusiveStartTableName != nil {
			listTablesInput.ExclusiveStartTableName = exclusiveStartTableName
		}

		listOut, err := c.DynamoDB.ListTables(ctx, listTablesInput)
		if err != nil {
			log.Fatalf("ListTables error: %v", err)
		}
		if len(listOut.TableNames) == 0 {
			fmt.Println("No DynamoDB tables found.")
			return allTables, nil
		}

		allTables = append(allTables, listOut.TableNames...)
		fmt.Println("Got tables so far: ", len(allTables))
		if listOut.LastEvaluatedTableName == nil {
			break
		}
		exclusiveStartTableName = listOut.LastEvaluatedTableName

	}
	return allTables, nil
}

func (c *AWSClient) ProcessTable(ctx context.Context, timeFrameDays int, tableName string) TableInfo {
	desc, err := c.DynamoDB.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		log.Printf("Error describing table %s: %v\n", tableName, err)
		return TableInfo{}
	}

	t := desc.Table
	billing := "PROVISIONED"
	if t.BillingModeSummary != nil {
		billing = string(t.BillingModeSummary.BillingMode)
	}

	var readCap, writeCap int64
	if billing == "PROVISIONED" && t.ProvisionedThroughput != nil {
		readCap = *t.ProvisionedThroughput.ReadCapacityUnits
		writeCap = *t.ProvisionedThroughput.WriteCapacityUnits
	}

	start := time.Now().Add(-time.Duration(timeFrameDays) * 24 * time.Hour)
	end := time.Now()

	readUsage, err1 := getAvgMetric(ctx, c.CloudWatch, tableName, "ConsumedReadCapacityUnits", start, end)
	writeUsage, err2 := getAvgMetric(ctx, c.CloudWatch, tableName, "ConsumedWriteCapacityUnits", start, end)
	metricsAvailable := err1 == nil && err2 == nil

	tableInfo := TableInfo{
		TableName:          tableName,
		BillingMode:        billing,
		ItemCount:          *t.ItemCount,
		TableSizeMB:        *t.TableSizeBytes / 1024 / 1024,
		ReadCapacityUnits:  readCap,
		WriteCapacityUnits: writeCap,
		AvgConsumedRead:    readUsage,
		AvgConsumedWrite:   writeUsage,
		MetricsAvailable:   metricsAvailable,
		TableArn:           t.TableArn,
	}

	cost := estimateDynamoDBCost(
		float64(readCap),
		float64(writeCap),
		*t.TableSizeBytes,
		24*timeFrameDays,
		0, 0, 0,
	)

	tableInfo.EstimatedCost = fmt.Sprintf("$%.2f", cost)
	return tableInfo
}

func getAvgMetric(ctx context.Context, cw *cloudwatch.Client, tableName, metric string, start, end time.Time) (float64, error) {
	out, err := cw.GetMetricStatistics(ctx, &cloudwatch.GetMetricStatisticsInput{
		Namespace:  aws.String("AWS/DynamoDB"),
		MetricName: aws.String(metric),
		Dimensions: []types.Dimension{
			{
				Name:  aws.String("TableName"),
				Value: aws.String(tableName),
			},
		},
		StartTime: aws.Time(start),
		EndTime:   aws.Time(end),
		Period:    aws.Int32(3600), // hourly average
		Statistics: []types.Statistic{
			types.StatisticAverage,
		},
	})
	if err != nil {
		return 0, err
	}

	if len(out.Datapoints) == 0 {
		return 0, fmt.Errorf("no data")
	}

	var total float64
	for _, dp := range out.Datapoints {
		total += *dp.Average
	}
	return total / float64(len(out.Datapoints)), nil
}

func estimateDynamoDBCost(readUnits, writeUnits float64, storageBytes int64, hours int, rcuPrice, wcuPrice, storagePricePerGBMonth float64) float64 {
	// Default prices (us-east-1) if zero
	if rcuPrice == 0 {
		rcuPrice = 0.00013 // $ per RCU-hour
	}
	if wcuPrice == 0 {
		wcuPrice = 0.00065 // $ per WCU-hour
	}
	if storagePricePerGBMonth == 0 {
		storagePricePerGBMonth = 0.25 // $ per GB-month
	}

	// Storage in GB
	storageGB := float64(storageBytes) / 1024.0 / 1024.0 / 1024.0

	// RCUs and WCUs are per hour
	// Total hours = period in hours
	rcuCost := readUnits * rcuPrice * float64(hours)
	wcuCost := writeUnits * wcuPrice * float64(hours)

	// Storage is monthly price; prorate for 30 days
	storageCost := storageGB * storagePricePerGBMonth * (float64(hours) / (24 * 30))

	total := rcuCost + wcuCost + storageCost
	return total
}
