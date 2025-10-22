package rds

import (
	"context"
	awsclient "cost-optimisation/src/aws"
	"cost-optimisation/src/shared/constants"
	"cost-optimisation/src/storage"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
)

const (
	ROOT        = "/Users/c-andrew.mironov/Work/cost-optimisation/"
	TABLES_PATH = ROOT + "data/rds"
	TIMEFRAME   = 14
)

func AnalyzeRDS() {
	ctx := context.Background()
	client := awsclient.NewAWSClient(awsclient.AWSClientOpts{
		Region:        constants.US_WEST_2,
		TimeFrameDays: TIMEFRAME,
	})

	rdsMetadata := extractRDSInfo(ctx, client)
	storage.WriteToJSON(TABLES_PATH, rdsMetadata)
	storage.WriteToCSV(TABLES_PATH, rdsMetadata)
}

func extractRDSInfo(ctx context.Context, client *awsclient.AWSClient) []RDSInfo {
	log.Println("Fetching RDS Metadata...")

	wg := sync.WaitGroup{}
	ch := make(chan RDSInfo)
	rdsInfoList := []RDSInfo{}

	for _, instanceID := range client.GetRDSInstances(ctx) {
		wg.Go(func() {
			ch <- ProcessRDSInstance(ctx, client, instanceID)
		})
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	for info := range ch {
		rdsInfoList = append(rdsInfoList, info)
	}

	return rdsInfoList
}

func ProcessRDSInstance(ctx context.Context, client *awsclient.AWSClient, instanceID string) RDSInfo {
	out, err := client.RDS.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(instanceID),
	})
	if err != nil {
		log.Printf("Error describing RDS instance %s: %v\n", instanceID, err)
		return RDSInfo{}
	}
	if len(out.DBInstances) == 0 {
		log.Printf("RDS instance %s not found\n", instanceID)
		return RDSInfo{}
	}

	inst := out.DBInstances[0]
	start := time.Now().Add(-24 * time.Hour * TIMEFRAME)
	end := time.Now()

	cpuAvg, _ := GetRDSMetric(ctx, client.CloudWatch, instanceID, "CPUUtilization", start, end)
	storageFree, _ := GetRDSMetric(ctx, client.CloudWatch, instanceID, "FreeStorageSpace", start, end)
	connections, _ := GetRDSMetric(ctx, client.CloudWatch, instanceID, "DatabaseConnections", start, end)
	readIOPS, _ := GetRDSMetric(ctx, client.CloudWatch, instanceID, "ReadIOPS", start, end)
	writeIOPS, _ := GetRDSMetric(ctx, client.CloudWatch, instanceID, "WriteIOPS", start, end)

	ti := RDSInfo{
		TableName:        instanceID,
		TableArn:         inst.DBInstanceArn,
		InstanceType:     *inst.DBInstanceClass,
		BillingMode:      *inst.Engine,
		Connections:      connections,
		TableSizeMB:      storageFree / 1024 / 1024,
		AvgConsumedRead:  readIOPS,
		AvgConsumedWrite: writeIOPS,
		UtilizationPct:   cpuAvg,
	}

	tableSizeGB := storageFree / 1024 / 1024 / 1024 // convert bytes → GB
	replicas := len(inst.ReadReplicaDBInstanceIdentifiers)
	multiAZ := inst.MultiAZ != nil && *inst.MultiAZ

	ti.EstimatedCost = EstimateRDSMonthlyCost(
		*inst.DBInstanceClass, // instanceType
		*inst.Engine,          // engine
		*inst.StorageType,     // storageType
		tableSizeGB,           // tableSizeGB
		readIOPS,              // readIOPS
		writeIOPS,             // writeIOPS
		replicas,              // replicas
		multiAZ,               // multiAZ
		connections,           // connections
	)

	ti.Recommendation = RecommendRDS(cpuAvg, storageFree)
	ti.NeedOptimisation = cpuAvg < 10.0 || cpuAvg > 80.0

	return ti
}

func EstimateRDSMonthlyCost(
	instanceType, engine, storageType string,
	tableSizeGB, readIOPS, writeIOPS float64,
	replicas int, multiAZ bool, connections float64,
) float64 {

	var hourlyRate float64
	switch {
	case strings.Contains(instanceType, "t3.micro"):
		hourlyRate = 0.017
	case strings.Contains(instanceType, "r6g.large"):
		hourlyRate = 0.188
	case strings.Contains(instanceType, "r6g.xlarge"):
		hourlyRate = 0.376
	case strings.Contains(instanceType, "r6g.2xlarge"):
		hourlyRate = 0.752
	case strings.Contains(instanceType, "r6g.4xlarge"):
		hourlyRate = 1.504
	case strings.Contains(instanceType, "serverless"):
		// Aurora Serverless v2 rough guess
		acus := math.Max(2, connections/500.0)
		return acus * 0.12 * 720
	default:
		hourlyRate = 0.10
	}

	computeCost := hourlyRate * 720 * float64(replicas)

	storageRate := 0.10
	if engine != "aurora-mysql" && engine != "aurora-postgresql" {
		storageRate = 0.25
	}
	storageCost := tableSizeGB * storageRate
	if multiAZ {
		storageCost *= 2
	}

	iopsCost := 0.0
	if storageType == "io1" {
		iopsCost = (readIOPS + writeIOPS) * 0.10
	}

	total := (computeCost + storageCost + iopsCost) * 1.25 // +25% overhead
	return total
}
