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

package fake

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/testing"
	"k8s.io/metrics/pkg/apis/external_metrics/v1beta1"
	eclient "k8s.io/metrics/pkg/client/external_metrics"
)

type GetForActionImpl struct {
	testing.GetAction
	MetricName     string
	MetricSelector metav1.LabelSelector
}

type GetForAction interface {
	testing.GetAction
	GetMetricName() string
	GetMetricSelector() metav1.LabelSelector
}

func (i GetForActionImpl) GetMetricName() string {
	return i.MetricName
}

func (i GetForActionImpl) GetMetricSelector() metav1.LabelSelector {
	return i.MetricSelector
}

func (i GetForActionImpl) GetSubresource() string {
	return i.MetricName
}

func NewGetForAction(namespace string, metricName string, labelSelector metav1.LabelSelector) GetForActionImpl {
	resource := schema.GroupResource{
		Group:    v1beta1.SchemeGroupVersion.Group,
		Resource: "externalmetrics",
	}
	return GetForActionImpl{
		GetAction:      testing.NewGetAction(resource.WithVersion(""), namespace, metricName),
		MetricName:     metricName,
		MetricSelector: labelSelector,
	}
}

type FakeExternalMetricsClient struct {
	testing.Fake
}

func (c *FakeExternalMetricsClient) NamespacedMetrics(namespace string) eclient.MetricsInterface {
	return &fakeNamespacedMetrics{
		Fake: c,
		ns:   namespace,
	}
}

type fakeNamespacedMetrics struct {
	Fake *FakeExternalMetricsClient
	ns   string
}

func (m *fakeNamespacedMetrics) Get(metricName string, metricSelector metav1.LabelSelector) (*v1beta1.ExternalMetricValueList, error) {
	obj, err := m.Fake.
		Invokes(NewGetForAction(m.ns, metricName, metricSelector), &v1beta1.ExternalMetricValueList{})

	if obj == nil {
		return nil, err
	}

	return obj.(*v1beta1.ExternalMetricValueList), err
}
