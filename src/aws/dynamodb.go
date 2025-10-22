package awsclient

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
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

func GetAvgMetric(ctx context.Context, cw *cloudwatch.Client, tableName, metric string, start, end time.Time) (float64, error) {
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

func EstimateDynamoDBCost(readUnits, writeUnits float64, storageBytes int64, hours int, rcuPrice, wcuPrice, storagePricePerGBMonth float64) float64 {
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
