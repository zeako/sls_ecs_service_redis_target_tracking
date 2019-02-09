package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/ecs"
	"github.com/go-redis/redis"
	"log"
)

var (
	sess             = session.Must(session.NewSession())
	ecsClient        = ecs.New(sess)
	cloudwatchClient = cloudwatch.New(sess)
)

type RedisEcsEvent struct {
	ClusterName   string `json:"clusterName"`
	RedisAddress  string `json:"redisAddress"`
	ServiceName   string `json:"serviceName"`
	SortedSetName string `json:"sortedSetName"`
}

func sortedSetLength(event RedisEcsEvent) (int64, error) {
	r := redis.NewClient(&redis.Options{
		Addr: event.RedisAddress,
	})
	res, err := r.ZCard(event.SortedSetName).Result()
	if err != nil {
		return 0, err
	}
	log.Printf("Number of items in sorted-set %s: %d", event.SortedSetName, res)
	return res, nil
}

func serviceRunningTasks(event RedisEcsEvent) (int64, error) {
	input := &ecs.DescribeServicesInput{
		Cluster: aws.String(event.ClusterName),
		Services: []*string{
			aws.String(event.ServiceName),
		},
	}
	res, err := ecsClient.DescribeServices(input)
	if err != nil {
		return 0, err
	}
	runningCount := *res.Services[0].RunningCount
	log.Printf("Number of running tasks for service %s: %d", event.ServiceName, runningCount)
	return runningCount, nil
}

func estimateBacklog(messages, tasks int64) int64 {
	var backlog int64
	if tasks <= 1 {
		backlog = messages
	} else {
		backlog = messages / tasks
	}

	// AWS target tracking limitation workaround
	// if below 1, AWS managed alarms state is set to INSUFFICIENT_DATA
	// and scale-in logic doesn't trigger
	if backlog < 1 {
		backlog = 1
	}
	log.Printf("Estimated backlog per instance: %d", backlog)
	return backlog
}

func putMetric(backlog int64, event RedisEcsEvent) error {
	input := &cloudwatch.PutMetricDataInput{
		Namespace: aws.String("ELASTICACHE_ECS"),
		MetricData: []*cloudwatch.MetricDatum{{
			MetricName: aws.String("RedisEcsServiceBacklog"),
			Dimensions: []*cloudwatch.Dimension{
				{
					Name:  aws.String("ClusterName"),
					Value: aws.String(event.ClusterName),
				},
				{
					Name:  aws.String("ServiceName"),
					Value: aws.String(event.ServiceName),
				},
				{
					Name:  aws.String("SortedSetName"),
					Value: aws.String(event.SortedSetName),
				},
			},
			Value: aws.Float64(float64(backlog)),
		}},
	}
	_, err := cloudwatchClient.PutMetricData(input)
	return err
}

func Handler(event RedisEcsEvent) error {
	messages, err := sortedSetLength(event)
	if err != nil {
		return err
	}

	tasks, err := serviceRunningTasks(event)
	if err != nil {
		return err
	}

	backlog := estimateBacklog(messages, tasks)
	return putMetric(backlog, event)
}

func main() {
	lambda.Start(Handler)
}
