package rds

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

type RDSInfo struct {
	TableName        string  `json:"tableName"`
	TableArn         *string `json:"tableArn"`
	BillingMode      string  `json:"billingMode"`
	InstanceType     string  `json:"instanceType"`
	Connections      float64 `json:"connections"`
	TableSizeMB      float64 `json:"tableSizeMB"`
	AvgConsumedRead  float64 `json:"avgConsumedRead"`
	AvgConsumedWrite float64 `json:"avgConsumedWrite"`
	UtilizationPct   float64 `json:"utilizationPct"`
	EstimatedCost    float64 `json:"estimatedCost"`
	NeedOptimisation bool    `json:"needOptimisation"`
	Recommendation   string  `json:"recommendation"`
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
