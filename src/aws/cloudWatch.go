package awsclient

import "math"

type CloudWatchMetricInfo struct {
	Namespace     string   `json:"namespace"`
	MetricName    string   `json:"metricName"`
	Dimensions    []string `json:"dimensions"`
	StorageGB     float64  `json:"storageGB"`
	Requests      int64    `json:"requests"`
	EstimatedCost float64  `json:"estimatedCost"`
}

func (c *AWSClient) EstimateCloudWatchMonthlyCost(metricsCount int, apiRequests int64) float64 {
	const (
		metricCostPerMonth = 0.30 // $ per metric per month
		apiCostPer1000     = 0.01 // $ per 1,000 GetMetricData requests
	)

	metricsCost := float64(metricsCount) * metricCostPerMonth
	apiCost := (float64(apiRequests) / 1000.0) * apiCostPer1000

	return math.Round((metricsCost+apiCost)*100) / 100 // rounded to cents
}
