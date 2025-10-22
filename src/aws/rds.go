package awsclient

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

type RDSInfo struct {
	TableName          string  `json:"tableName"`
	BillingMode        string  `json:"billingMode"`
	InstanceType       string  `json:"instanceType"`
	Connections        float64 `json:"connections"`
	ItemCount          int64   `json:"itemCount"`
	TableSizeMB        float64 `json:"tableSizeMB"`
	ReadCapacityUnits  int64   `json:"readCapacityUnits"`
	WriteCapacityUnits int64   `json:"writeCapacityUnits"`
	AvgConsumedRead    float64 `json:"avgConsumedRead"`
	AvgConsumedWrite   float64 `json:"avgConsumedWrite"`
	MetricsAvailable   bool    `json:"metricsAvailable"`
	TableArn           *string `json:"tableArn"`
	EstimatedCost      float64 `json:"estimatedCost"`
	UtilizationPct     float64 `json:"utilizationPct"`
	CurrentCost        float64 `json:"currentCost"`
	ActualCost         float64 `json:"actualCost"`
	PotentialSavings   float64 `json:"potentialSavings"`
	PotentialSavingsP  float64 `json:"potentialSavingsP"`
	Recommendation     string  `json:"recommendation"`
	NeedOptimisation   bool    `json:"needOptimisation"`
}

func GetRDSMetric(ctx context.Context, cw *cloudwatch.Client, instanceID, metricName string, start, end time.Time) (float64, error) {
	out, err := cw.GetMetricStatistics(ctx, &cloudwatch.GetMetricStatisticsInput{
		Namespace:  aws.String("AWS/RDS"),
		MetricName: aws.String(metricName),
		Dimensions: []types.Dimension{
			{Name: aws.String("DBInstanceIdentifier"), Value: aws.String(instanceID)},
		},
		StartTime: &start,
		EndTime:   &end,
		Period:    aws.Int32(3600), // hourly
		Statistics: []types.Statistic{
			types.StatisticAverage,
		},
	})
	if err != nil {
		return 0, fmt.Errorf("GetMetricStatistics for %s failed: %v", metricName, err)
	}

	if len(out.Datapoints) == 0 {
		return 0, fmt.Errorf("no datapoints for %s", metricName)
	}

	// Compute average across datapoints
	var total float64
	for _, dp := range out.Datapoints {
		total += *dp.Average
	}
	return total / float64(len(out.Datapoints)), nil
}

func RecommendRDS(cpu float64, freeStorage float64) string {
	if cpu < 10 {
		return "Consider downsizing or using Aurora Serverless"
	}
	if cpu > 80 {
		return "Consider upgrading instance class"
	}
	if freeStorage < 1024*1024*1024*10 { // less than 10 GB
		return "Low storage: increase allocated storage"
	}
	return "Configuration OK"
}
