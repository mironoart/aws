package awsclient

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/rds"
)

type AWSClientOpts struct {
	Region        string
	TimeFrameDays int
}

type AWSClient struct {
	DynamoDB   *dynamodb.Client
	CloudWatch *cloudwatch.Client
	RDS        *rds.Client
}

func NewAWSClient(opts AWSClientOpts) *AWSClient {
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(opts.Region))
	if err != nil {
		log.Fatalf("unable to load SDK config: %v", err)
	}

	return &AWSClient{
		DynamoDB:   dynamodb.NewFromConfig(cfg),
		CloudWatch: cloudwatch.NewFromConfig(cfg),
		RDS:        rds.NewFromConfig(cfg),
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

	readUsage, err1 := GetAvgMetric(ctx, c.CloudWatch, tableName, "ConsumedReadCapacityUnits", start, end)
	writeUsage, err2 := GetAvgMetric(ctx, c.CloudWatch, tableName, "ConsumedWriteCapacityUnits", start, end)
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

	cost := EstimateDynamoDBCost(
		float64(readCap),
		float64(writeCap),
		*t.TableSizeBytes,
		24*timeFrameDays,
		0, 0, 0,
	)

	tableInfo.EstimatedCost = fmt.Sprintf("$%.2f", cost)
	return tableInfo
}

// -------------------
// RDS FUNCTIONS
// -------------------

func (c *AWSClient) GetRDSInstances(ctx context.Context) []string {
	log.Println("Fetching RDS instancess...")
	var allInstances []string
	var marker *string

	for {
		out, err := c.RDS.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{
			Marker: marker,
		})
		if err != nil {
			log.Fatal("Failed to describe RDS instances")
		}

		for _, inst := range out.DBInstances {
			allInstances = append(allInstances, aws.ToString(inst.DBInstanceIdentifier))
		}

		if out.Marker == nil {
			break
		}
		marker = out.Marker
	}

	log.Printf("Got RDS instances %d", len(allInstances))
	return allInstances
}

func (c *AWSClient) GetCloudWatchMetrics(ctx context.Context, namespace string) ([]CloudWatchMetricInfo, error) {
	input := &cloudwatch.ListMetricsInput{
		Namespace: &namespace,
	}

	var metrics []CloudWatchMetricInfo
	paginator := cloudwatch.NewListMetricsPaginator(c.CloudWatch, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list metrics: %w", err)
		}

		for _, m := range page.Metrics {
			info := CloudWatchMetricInfo{
				Namespace:  *m.Namespace,
				MetricName: *m.MetricName,
			}
			for _, d := range m.Dimensions {
				info.Dimensions = append(info.Dimensions, fmt.Sprintf("%s=%s", *d.Name, *d.Value))
			}
			metrics = append(metrics, info)
		}
	}

	return metrics, nil
}
