/*
Copyright 2018 The Kubernetes Authors.

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
	"fmt"
	"math"
	"time"

	as "k8s.io/api/autoscaling/v2beta1"
	corev1 "k8s.io/api/core/v1"
	extensions "k8s.io/api/extensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/instrumentation/monitoring"

	. "github.com/onsi/ginkgo"
)

var (
	externalMetricValue      = int64(85)
	otherExternalMetricValue = int64(44)
	dummyDeploymentName      = "dummy-deployment"
)

var _ = SIGDescribe("[HPA] Horizontal pod autoscaling (scale resource: Custom Metrics)", func() {
	BeforeEach(func() {
		framework.SkipUnlessProviderIs("gce", "gke")
	})

	f := framework.NewDefaultFramework("horizontal-pod-autoscaling")

	It("should scale down with External Metric with target value [Feature:CustomMetricsAutoscaling]", func() {
		initialReplicas := 2
		// metric should cause scale down
		metricValue := externalMetricValue
		metricTarget := 2 * metricValue
		metricTargets := map[string]externalMetricTarget{
			"my-external-metric": {
				value:     metricTarget,
				isAverage: false,
				selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"foo": "bar",
					},
				},
			},
		}
		tc := CustomMetricsTestCase{
			framework:       f,
			kubeClient:      f.ClientSet,
			provider:        &customMetricsFakeApiserver{},
			initialReplicas: initialReplicas,
			scaledReplicas:  1,
			deployment:      framework.NewDeployment(dummyDeploymentName, int32(initialReplicas), map[string]string{"name": dummyDeploymentName}, "pause", framework.GetPauseImageName(f.ClientSet), extensions.RollingUpdateDeploymentStrategyType),
			hpa:             externalHPA(f.Namespace.ObjectMeta.Name, metricTargets)}
		tc.Run()
	})

	It("should scale down with External Metric with target average value [Feature:CustomMetricsAutoscaling]", func() {
		initialReplicas := 2
		// metric should cause scale down
		metricValue := externalMetricValue
		metricAverageTarget := 2 * metricValue
		metricTargets := map[string]externalMetricTarget{
			"my-external-metric": {
				value:     metricAverageTarget,
				isAverage: true,
				selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"foo": "bar",
					},
				},
			},
		}
		tc := CustomMetricsTestCase{
			framework:       f,
			kubeClient:      f.ClientSet,
			provider:        &customMetricsFakeApiserver{},
			initialReplicas: initialReplicas,
			scaledReplicas:  1,
			deployment:      framework.NewDeployment(dummyDeploymentName, int32(initialReplicas), map[string]string{"name": dummyDeploymentName}, "pause", framework.GetPauseImageName(f.ClientSet), extensions.RollingUpdateDeploymentStrategyType),
			hpa:             externalHPA(f.Namespace.ObjectMeta.Name, metricTargets)}
		tc.Run()
	})

	It("should scale up with two External metrics [Feature:CustomMetricsAutoscaling]", func() {
		initialReplicas := 1
		// metric 1 would cause a scale down, if not for metric 2
		metric1Value := externalMetricValue
		metric1Target := 2 * metric1Value
		// metric2 should cause a scale up
		metric2Value := otherExternalMetricValue
		metric2Target := int64(math.Ceil(0.5 * float64(metric2Value)))
		metricTargets := map[string]externalMetricTarget{
			"my-external-metric": {
				value:     metric1Target,
				isAverage: false,
				selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"foo": "bar",
					},
				},
			},
			"other-external-metric": {
				value:     metric2Target,
				isAverage: false,
			},
		}
		tc := CustomMetricsTestCase{
			framework:       f,
			kubeClient:      f.ClientSet,
			provider:        &customMetricsFakeApiserver{},
			initialReplicas: initialReplicas,
			scaledReplicas:  3,
			deployment:      framework.NewDeployment(dummyDeploymentName, int32(initialReplicas), map[string]string{"name": dummyDeploymentName}, "pause", framework.GetPauseImageName(f.ClientSet), extensions.RollingUpdateDeploymentStrategyType),
			hpa:             externalHPA(f.Namespace.ObjectMeta.Name, metricTargets)}
		tc.Run()
	})
})

// CustomMetricsTestProvider is the provider of custom metrics for Horizontal
// Pod Autoscaling tests. The implementations shouls provide the custom and
// external metrics API implementations so that the tests can use some custom
// and external metric values to scale on.
type CustomMetricsTestProvider interface {
	CleanupHooks() []func()
	Setup() error
}

func cleanupProvider(tp CustomMetricsTestProvider) {
	for _, hook := range tp.CleanupHooks() {
		hook()
	}
}

type CustomMetricsTestCase struct {
	description     string
	framework       *framework.Framework
	provider        CustomMetricsTestProvider
	hpa             *as.HorizontalPodAutoscaler
	kubeClient      clientset.Interface
	deployment      *extensions.Deployment
	pod             *corev1.Pod
	initialReplicas int
	scaledReplicas  int
}

func (tc *CustomMetricsTestCase) Run() {
	if tc.provider == nil {
		framework.Failf("Missing custom metrics test provider")
	}
	defer cleanupProvider(tc.provider)
	err := tc.provider.Setup()
	if err != nil {
		framework.Failf("Failed to set up custom metrics test provider: %v", err)
	}

	// Run application that exports the metric
	err = createDeploymentToScale(tc.framework, tc.kubeClient, tc.deployment, tc.pod)
	if err != nil {
		framework.Failf("Failed to create stackdriver-exporter pod: %v", err)
	}
	defer cleanupDeploymentsToScale(tc.framework, tc.kubeClient, tc.deployment, tc.pod)

	// Wait for the deployment to run
	waitForReplicas(tc.deployment.ObjectMeta.Name, tc.framework.Namespace.ObjectMeta.Name, tc.kubeClient, 15*time.Minute, tc.initialReplicas)

	// Autoscale the deployment
	_, err = tc.kubeClient.AutoscalingV2beta1().HorizontalPodAutoscalers(tc.framework.Namespace.ObjectMeta.Name).Create(tc.hpa)
	if err != nil {
		framework.Failf("Failed to create HPA: %v", err)
	}
	defer tc.kubeClient.AutoscalingV2beta1().HorizontalPodAutoscalers(tc.framework.Namespace.ObjectMeta.Name).Delete(tc.hpa.ObjectMeta.Name, &metav1.DeleteOptions{})

	waitForReplicas(tc.deployment.ObjectMeta.Name, tc.framework.Namespace.ObjectMeta.Name, tc.kubeClient, 15*time.Minute, tc.scaledReplicas)
}

func createDeploymentToScale(f *framework.Framework, cs clientset.Interface, deployment *extensions.Deployment, pod *corev1.Pod) error {
	if deployment != nil {
		_, err := cs.Extensions().Deployments(f.Namespace.ObjectMeta.Name).Create(deployment)
		if err != nil {
			return err
		}
	}
	if pod != nil {
		_, err := cs.CoreV1().Pods(f.Namespace.ObjectMeta.Name).Create(pod)
		if err != nil {
			return err
		}
	}
	return nil
}

func cleanupDeploymentsToScale(f *framework.Framework, cs clientset.Interface, deployment *extensions.Deployment, pod *corev1.Pod) {
	if deployment != nil {
		_ = cs.Extensions().Deployments(f.Namespace.ObjectMeta.Name).Delete(deployment.ObjectMeta.Name, &metav1.DeleteOptions{})
	}
	if pod != nil {
		_ = cs.CoreV1().Pods(f.Namespace.ObjectMeta.Name).Delete(pod.ObjectMeta.Name, &metav1.DeleteOptions{})
	}
}

func waitForReplicas(deploymentName, namespace string, cs clientset.Interface, timeout time.Duration, desiredReplicas int) {
	interval := 20 * time.Second
	err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		deployment, err := cs.ExtensionsV1beta1().Deployments(namespace).Get(deploymentName, metav1.GetOptions{})
		if err != nil {
			framework.Failf("Failed to get deployment %s: %v", deployment, err)
		}
		replicas := int(deployment.Status.ReadyReplicas)
		framework.Logf("waiting for %d replicas (current: %d)", desiredReplicas, replicas)
		return replicas == desiredReplicas, nil // Expected number of replicas found. Exit.
	})
	if err != nil {
		framework.Failf("Timeout waiting %v for %v replicas", timeout, desiredReplicas)
	}
}

func externalHPA(namespace string, metricTargets map[string]externalMetricTarget) *as.HorizontalPodAutoscaler {
	var minReplicas int32 = 1
	metricSpecs := []as.MetricSpec{}
	for metric, target := range metricTargets {
		var metricSpec as.MetricSpec
		metricSpec = as.MetricSpec{
			Type: as.ExternalMetricSourceType,
			External: &as.ExternalMetricSource{
				MetricName:     metric,
				MetricSelector: target.selector,
			},
		}
		if target.isAverage {
			metricSpec.External.TargetAverageValue = resource.NewQuantity(target.value, resource.DecimalSI)
		} else {
			metricSpec.External.TargetValue = resource.NewQuantity(target.value, resource.DecimalSI)
		}
		metricSpecs = append(metricSpecs, metricSpec)
	}
	hpa := &as.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "custom-metrics-external-hpa",
			Namespace: namespace,
		},
		Spec: as.HorizontalPodAutoscalerSpec{
			Metrics:     metricSpecs,
			MaxReplicas: 3,
			MinReplicas: &minReplicas,
			ScaleTargetRef: as.CrossVersionObjectReference{
				APIVersion: "extensions/v1beta1",
				Kind:       "Deployment",
				Name:       dummyDeploymentName,
			},
		},
	}

	return hpa
}

type customMetricsFakeApiserver struct {
	cleanupHooks []func()
	adapter      string
}

func (a *customMetricsFakeApiserver) Setup() error {
	err := monitoring.CreateAdapter(monitoring.FakeAdapterLocation, monitoring.FakeAdapter)
	if err != nil {
		return fmt.Errorf("Failed to create custom metrics fake apiserver: %v", err)
	}
	a.cleanupHooks = append(a.cleanupHooks, func() { monitoring.CleanupAdapter(monitoring.FakeAdapter) })
	return nil
}

func (a *customMetricsFakeApiserver) CleanupHooks() []func() {
	return a.cleanupHooks
}
