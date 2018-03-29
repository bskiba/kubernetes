/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package autoscaling

import (
	"context"
	"fmt"
	"math"

	gcm "google.golang.org/api/monitoring/v3"
	as "k8s.io/api/autoscaling/v2beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/instrumentation/monitoring"

	. "github.com/onsi/ginkgo"
	"golang.org/x/oauth2/google"
)

const (
	stackdriverExporterDeployment = "stackdriver-exporter-deployment"
	stackdriverExporterPod        = "stackdriver-exporter-pod"
)

var _ = SIGDescribe("[HPA] Horizontal pod autoscaling (scale resource: Custom Metrics from Stackdriver)", func() {
	BeforeEach(func() {
		framework.SkipUnlessProviderIs("gce", "gke")
	})

	f := framework.NewDefaultFramework("horizontal-pod-autoscaling")

	It("should scale down with Custom Metric of type Pod from Stackdriver [Feature:CustomMetricsAutoscalingStackdriver]", func() {
		initialReplicas := 2
		// metric should cause scale down
		metricValue := int64(100)
		metricTarget := 2 * metricValue
		tc := CustomMetricsTestCase{
			framework:       f,
			kubeClient:      f.ClientSet,
			provider:        &stackdriverTestProvider{},
			initialReplicas: initialReplicas,
			scaledReplicas:  1,
			deployment:      monitoring.SimpleStackdriverExporterDeployment(stackdriverExporterDeployment, f.Namespace.ObjectMeta.Name, int32(initialReplicas), metricValue),
			hpa:             simplePodsHPA(f.Namespace.ObjectMeta.Name, metricTarget)}
		tc.Run()
	})

	It("should scale down with Custom Metric of type Object from Stackdriver [Feature:CustomMetricsAutoscalingStackdriver]", func() {
		initialReplicas := 2
		// metric should cause scale down
		metricValue := int64(100)
		metricTarget := 2 * metricValue
		tc := CustomMetricsTestCase{
			framework:       f,
			kubeClient:      f.ClientSet,
			provider:        &stackdriverTestProvider{},
			initialReplicas: initialReplicas,
			scaledReplicas:  1,
			// Metric exported by deployment is ignored
			deployment: monitoring.SimpleStackdriverExporterDeployment(dummyDeploymentName, f.Namespace.ObjectMeta.Name, int32(initialReplicas), 0 /* ignored */),
			pod:        monitoring.StackdriverExporterPod(stackdriverExporterPod, f.Namespace.Name, stackdriverExporterPod, monitoring.CustomMetricName, metricValue),
			hpa:        objectHPA(f.Namespace.ObjectMeta.Name, metricTarget)}
		tc.Run()
	})

	It("should scale down with External Metric with target value from Stackdriver [Feature:CustomMetricsAutoscalingStackdriver]", func() {
		initialReplicas := 2
		// metric should cause scale down
		metricValue := externalMetricValue
		metricTarget := 2 * metricValue
		metricTargets := map[string]externalMetricTarget{
			stackdriverMetricName("target"): {
				value:     metricTarget,
				isAverage: false,
				selector:  getStackdriverSelector(),
			},
		}
		tc := CustomMetricsTestCase{
			framework:       f,
			kubeClient:      f.ClientSet,
			provider:        &stackdriverTestProvider{},
			initialReplicas: initialReplicas,
			scaledReplicas:  1,
			// Metric exported by deployment is ignored
			deployment: monitoring.SimpleStackdriverExporterDeployment(dummyDeploymentName, f.Namespace.ObjectMeta.Name, int32(initialReplicas), 0 /* ignored */),
			pod:        monitoring.StackdriverExporterPod(stackdriverExporterPod, f.Namespace.Name, stackdriverExporterPod, "target", metricValue),
			hpa:        externalHPA(f.Namespace.ObjectMeta.Name, metricTargets)}
		tc.Run()
	})

	It("should scale down with External Metric with target average value from Stackdriver [Feature:CustomMetricsAutoscalingStackdriver]", func() {
		initialReplicas := 2
		// metric should cause scale down
		metricValue := externalMetricValue
		metricAverageTarget := 2 * metricValue
		metricTargets := map[string]externalMetricTarget{
			stackdriverMetricName("target_average"): {
				value:     metricAverageTarget,
				isAverage: true,
				selector:  getStackdriverSelector(),
			},
		}
		tc := CustomMetricsTestCase{
			framework:       f,
			kubeClient:      f.ClientSet,
			provider:        &stackdriverTestProvider{},
			initialReplicas: initialReplicas,
			scaledReplicas:  1,
			// Metric exported by deployment is ignored
			deployment: monitoring.SimpleStackdriverExporterDeployment(dummyDeploymentName, f.Namespace.ObjectMeta.Name, int32(initialReplicas), 0 /* ignored */),
			pod:        monitoring.StackdriverExporterPod(stackdriverExporterPod, f.Namespace.Name, stackdriverExporterPod, "target_average", externalMetricValue),
			hpa:        externalHPA(f.Namespace.ObjectMeta.Name, metricTargets)}
		tc.Run()
	})

	It("should scale down with Custom Metric of type Pod from Stackdriver with Prometheus [Feature:CustomMetricsAutoscalingStackdriver]", func() {
		initialReplicas := 2
		// metric should cause scale down
		metricValue := int64(100)
		metricTarget := 2 * metricValue
		tc := CustomMetricsTestCase{
			framework:       f,
			kubeClient:      f.ClientSet,
			provider:        &stackdriverTestProvider{},
			initialReplicas: initialReplicas,
			scaledReplicas:  1,
			deployment:      monitoring.PrometheusExporterDeployment(stackdriverExporterDeployment, f.Namespace.ObjectMeta.Name, int32(initialReplicas), metricValue),
			hpa:             simplePodsHPA(f.Namespace.ObjectMeta.Name, metricTarget)}
		tc.Run()
	})

	It("should scale up with two metrics of type Pod from Stackdriver [Feature:CustomMetricsAutoscalingStackdriver]", func() {
		initialReplicas := 1
		// metric 1 would cause a scale down, if not for metric 2
		metric1Value := int64(100)
		metric1Target := 2 * metric1Value
		// metric2 should cause a scale up
		metric2Value := int64(200)
		metric2Target := int64(0.5 * float64(metric2Value))
		containers := []monitoring.CustomMetricContainerSpec{
			{
				Name:        "stackdriver-exporter-metric1",
				MetricName:  "metric1",
				MetricValue: metric1Value,
			},
			{
				Name:        "stackdriver-exporter-metric2",
				MetricName:  "metric2",
				MetricValue: metric2Value,
			},
		}
		metricTargets := map[string]int64{"metric1": metric1Target, "metric2": metric2Target}
		tc := CustomMetricsTestCase{
			framework:       f,
			kubeClient:      f.ClientSet,
			provider:        &stackdriverTestProvider{},
			initialReplicas: initialReplicas,
			scaledReplicas:  3,
			deployment:      monitoring.StackdriverExporterDeployment(stackdriverExporterDeployment, f.Namespace.ObjectMeta.Name, int32(initialReplicas), containers),
			hpa:             podsHPA(f.Namespace.ObjectMeta.Name, stackdriverExporterDeployment, metricTargets)}
		tc.Run()
	})

	It("should scale up with two External metrics from Stackdriver [Feature:CustomMetricsAutoscalingStackdriver]", func() {
		initialReplicas := 1
		// metric 1 would cause a scale down, if not for metric 2
		metric1Value := externalMetricValue
		metric1Target := 2 * metric1Value
		// metric2 should cause a scale up
		metric2Value := externalMetricValue
		metric2Target := int64(math.Ceil(0.5 * float64(metric2Value)))
		metricTargets := map[string]externalMetricTarget{
			stackdriverMetricName("external_metric_1"): {
				value:     metric1Target,
				isAverage: false,
				selector:  getStackdriverSelector(),
			},
			stackdriverMetricName("external_metric_2"): {
				value:     metric2Target,
				isAverage: false,
				selector:  getStackdriverSelector(),
			},
		}
		containers := []monitoring.CustomMetricContainerSpec{
			{
				Name:        "stackdriver-exporter-metric1",
				MetricName:  "external_metric_1",
				MetricValue: metric1Value,
			},
			{
				Name:        "stackdriver-exporter-metric2",
				MetricName:  "external_metric_2",
				MetricValue: metric2Value,
			},
		}
		tc := CustomMetricsTestCase{
			framework:       f,
			kubeClient:      f.ClientSet,
			provider:        &stackdriverTestProvider{},
			initialReplicas: initialReplicas,
			scaledReplicas:  3,
			deployment:      monitoring.StackdriverExporterDeployment(dummyDeploymentName, f.Namespace.ObjectMeta.Name, int32(initialReplicas), containers),
			hpa:             externalHPA(f.Namespace.ObjectMeta.Name, metricTargets)}
		tc.Run()
	})
})

type stackdriverTestProvider struct {
	cleanupHooks []func()
}

func (tp *stackdriverTestProvider) Setup() error {
	projectId := framework.TestContext.CloudConfig.ProjectID

	ctx := context.Background()
	client, err := google.DefaultClient(ctx, gcm.CloudPlatformScope)

	// Hack for running tests locally, needed to authenticate in Stackdriver
	// If this is your use case, create application default credentials:
	// $ gcloud auth application-default login
	// and uncomment following lines:
	/*
		ts, err := google.DefaultTokenSource(oauth2.NoContext)
		framework.Logf("Couldn't get application default credentials, %v", err)
		if err != nil {
			framework.Failf("Error accessing application default credentials, %v", err)
		}
		client := oauth2.NewClient(oauth2.NoContext, ts)
	*/

	gcmService, err := gcm.New(client)
	if err != nil {
		return fmt.Errorf("Failed to create gcm service, %v", err)
	}

	// Set up a cluster: create a custom metric and set up k8s-sd adapter
	err = monitoring.CreateDescriptors(gcmService, projectId)
	if err != nil {
		return fmt.Errorf("Failed to create metric descriptor: %v", err)
	}
	tp.cleanupHooks = append(tp.cleanupHooks, func() { monitoring.CleanupDescriptors(gcmService, projectId) })

	err = monitoring.CreateStackdriverAdapter(monitoring.AdapterDefault)
	if err != nil {
		return fmt.Errorf("Failed to create adapter: %v", err)
	}
	tp.cleanupHooks = append(tp.cleanupHooks, func() { monitoring.CleanupAdapter(monitoring.AdapterDefault) })

	return nil
}

func (tp *stackdriverTestProvider) CleanupHooks() []func() {
	return tp.cleanupHooks
}

func simplePodsHPA(namespace string, metricTarget int64) *as.HorizontalPodAutoscaler {
	return podsHPA(namespace, stackdriverExporterDeployment, map[string]int64{monitoring.CustomMetricName: metricTarget})
}

func podsHPA(namespace string, deploymentName string, metricTargets map[string]int64) *as.HorizontalPodAutoscaler {
	var minReplicas int32 = 1
	metrics := []as.MetricSpec{}
	for metric, target := range metricTargets {
		metrics = append(metrics, as.MetricSpec{
			Type: as.PodsMetricSourceType,
			Pods: &as.PodsMetricSource{
				MetricName:         metric,
				TargetAverageValue: *resource.NewQuantity(target, resource.DecimalSI),
			},
		})
	}
	return &as.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "custom-metrics-pods-hpa",
			Namespace: namespace,
		},
		Spec: as.HorizontalPodAutoscalerSpec{
			Metrics:     metrics,
			MaxReplicas: 3,
			MinReplicas: &minReplicas,
			ScaleTargetRef: as.CrossVersionObjectReference{
				APIVersion: "extensions/v1beta1",
				Kind:       "Deployment",
				Name:       deploymentName,
			},
		},
	}
}

func objectHPA(namespace string, metricTarget int64) *as.HorizontalPodAutoscaler {
	var minReplicas int32 = 1
	return &as.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "custom-metrics-objects-hpa",
			Namespace: namespace,
		},
		Spec: as.HorizontalPodAutoscalerSpec{
			Metrics: []as.MetricSpec{
				{
					Type: as.ObjectMetricSourceType,
					Object: &as.ObjectMetricSource{
						MetricName: monitoring.CustomMetricName,
						Target: as.CrossVersionObjectReference{
							Kind: "Pod",
							Name: stackdriverExporterPod,
						},
						TargetValue: *resource.NewQuantity(metricTarget, resource.DecimalSI),
					},
				},
			},
			MaxReplicas: 3,
			MinReplicas: &minReplicas,
			ScaleTargetRef: as.CrossVersionObjectReference{
				APIVersion: "extensions/v1beta1",
				Kind:       "Deployment",
				Name:       dummyDeploymentName,
			},
		},
	}
}

type externalMetricTarget struct {
	value     int64
	isAverage bool
	selector  *metav1.LabelSelector
}

func getStackdriverSelector() *metav1.LabelSelector {
	return &metav1.LabelSelector{
		MatchLabels: map[string]string{"resource.type": "gke_container"},
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{
				Key:      "resource.labels.namespace_id",
				Operator: metav1.LabelSelectorOpIn,
				// TODO(bskiba): change default to real namespace name once it is available
				// from Stackdriver.
				Values: []string{"default", "dummy"},
			},
			{
				Key:      "resource.labels.pod_id",
				Operator: metav1.LabelSelectorOpExists,
				Values:   []string{},
			},
		},
	}
}

func stackdriverMetricName(metricName string) string {
	return "custom.googleapis.com|" + metricName
}
