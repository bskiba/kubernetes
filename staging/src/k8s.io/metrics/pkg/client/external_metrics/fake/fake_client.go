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
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/testing"
	"k8s.io/metrics/pkg/apis/custom_metrics/v1beta1"
	emclient "k8s.io/metrics/pkg/client/external_metrics"
)

type GetForActionImpl struct {
	testing.GetAction
	MetricName    string
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

func NewGetForAction(namespace, metricSelector metav1.LabelSelector, metricName string) GetForActionImpl {
	// the version doesn't matter
	gvk := groupKind.WithVersion("")
	gvr, _ := meta.UnsafeGuessKindToResource(gvk)
	groupResourceForKind := schema.GroupResource{
		Group:    gvr.Group,
		Resource: gvr.Resource,
	}
	resource := schema.GroupResource{
		Group:    v1beta1.SchemeGroupVersion.Group,
		Resource: groupResourceForKind.String(),
	}
	return GetForActionImpl{
		GetAction:     testing.NewGetAction(resource.WithVersion(""), namespace, name),
		MetricName:    metricName,
		MetricSelector: metricSelector,
	}
}

type FakeExternalMetricsClient struct {
	testing.Fake
}

func (c *FakeExternalMetricsClient) NamespacedMetrics(namespace string) emclient.MetricsInterface {
	return &fakeNamespacedMetrics{
		Fake: c,
		ns:   namespace,
	}
}

type fakeNamespacedMetrics struct {
	Fake *FakeExternalMetricsClient
	ns   string
}

func (m *fakeNamespacedMetrics) Get(metricSelector metav1.LabelSelector, metricName string) (*v1beta1.ExternalMetricValueList, error) {
	obj, err := m.Fake.
		Invokes(NewGetForAction(m.ns, metricSelector, metricName), &v1beta1.ExternalMetricValueList{})

	if obj == nil {
		return nil, err
	}

	return obj.(*v1beta1.ExternalMetricValueList), err
}
