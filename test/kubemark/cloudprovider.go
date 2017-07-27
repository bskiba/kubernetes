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

package kubemark

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/api"
	kube_client "k8s.io/kubernetes/pkg/client/clientset_generated/clientset"
	informers "k8s.io/kubernetes/pkg/client/informers/informers_generated/externalversions"

	"github.com/golang/glog"
)

const (
	namespaceKubemark = "kubemark"
	hollowNodeName    = "hollow-node"
	nodeGroupLabel    = "autoscaling.k8s.io/nodegroup"
)

// Provider
type Provider struct {
	nodeTemplate    *apiv1.ReplicationController
	externalCluster ExternalCluster
	kubemarkCluster KubemarkCluster
}

type ExternalCluster struct {
	rcLister  cache.SharedInformer
	podLister cache.SharedInformer
	client    kube_client.Interface
}

type KubemarkCluster struct {
	client            kube_client.Interface
	nodeLister        cache.SharedInformer
	nodesToDelete     map[string]bool
	nodesToDeleteLock sync.Mutex
}

func NewKubemarkProvider(externalClient kube_client.Interface, kubemarkClient kube_client.Interface, stop <-chan struct{}) (*Provider, error) {
	externalInformerFactory := informers.NewSharedInformerFactory(externalClient, 0)
	kubemarkInformerFactory := informers.NewSharedInformerFactory(kubemarkClient, 0)
	manager := &Provider{
		externalCluster: ExternalCluster{
			rcLister:  externalInformerFactory.InformerFor(&apiv1.ReplicationController{}, newReplicationControllerInformer),
			podLister: externalInformerFactory.InformerFor(&apiv1.Pod{}, newPodInformer),
			client:    externalClient,
		},
		kubemarkCluster: KubemarkCluster{
			nodeLister:        kubemarkInformerFactory.InformerFor(&apiv1.Node{}, newNodeInformer),
			client:            kubemarkClient,
			nodesToDelete:     make(map[string]bool),
			nodesToDeleteLock: sync.Mutex{},
		},
	}

	externalInformerFactory.Start(stop)
	for _, synced := range externalInformerFactory.WaitForCacheSync(stop) {
		if !synced {
			return nil, fmt.Errorf("Failed to sync data  of external cluster.")
		}
	}
	kubemarkInformerFactory.Start(stop)
	for _, synced := range kubemarkInformerFactory.WaitForCacheSync(stop) {
		if !synced {
			return nil, fmt.Errorf("Failed to sync data  of external cluster.")
		}
	}
	manager.kubemarkCluster.nodeLister.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: manager.kubemarkCluster.removeUnneededNodes,
	})

	manager.kubemarkCluster.nodesToDelete = make(map[string]bool)

	nodeTemplate, err := manager.getNodeTemplate()
	if err != nil {
		return nil, err
	}
	manager.nodeTemplate = nodeTemplate
	return manager, nil
}

func (kubemarkProvider *Provider) RegisterNodeGroup(nodeGroup string, size int) error {
	glog.V(2).Infof("Registering nodegroup %v", nodeGroup)
	ng := kubemarkProvider.getNodeGroupByName(nodeGroup)
	if ng == nil {
		for i := 0; i < size; i++ {
			if err := kubemarkProvider.addNodeToNodeGroup(nodeGroup); err != nil {
				return err
			}
		}
	}
	return nil
}

// GetNodeGroupForNode returns the name of the nodeGroup to which node with nodeName
// belongs. Returns an error if nodeGroup for node was not found.
func (kubemarkProvider *Provider) GetNodeGroupForNode(nodeName string) (string, error) {
	for _, podObj := range kubemarkProvider.externalCluster.podLister.GetStore().List() {
		pod := podObj.(*apiv1.Pod)
		if pod.ObjectMeta.Name == nodeName {
			//glog.Infof("returning pod Label %q", pod.ObjectMeta.Labels[nodeGroupLabel])
			//glog.Infof("Pod: %+v", pod)
			nodeGroup, ok := pod.ObjectMeta.Labels[nodeGroupLabel]
			if ok {
				return nodeGroup, nil
			}
			return "", fmt.Errorf("Can't find nodegroup for node %s. Node exists but does not have the nodeGroupName label.", nodeName)
		}
	}
	return "", fmt.Errorf("Can't find nodegroup for node %s", nodeName)
}

func (kubemarkProvider *Provider) DeleteNode(nodeGroup string, node string) error {
	for _, podObj := range kubemarkProvider.externalCluster.podLister.GetStore().List() {
		pod := podObj.(*apiv1.Pod)
		if pod.ObjectMeta.Name == node {
			if pod.ObjectMeta.Labels[nodeGroupLabel] != nodeGroup {
				return fmt.Errorf("Can't delete node %s from nodegroup %s. Node is not in nodegroup.", node, nodeGroup)
			}
			policy := metav1.DeletePropagationForeground
			err := kubemarkProvider.externalCluster.client.CoreV1().ReplicationControllers(namespaceKubemark).Delete(
				pod.ObjectMeta.Labels["name"],
				&metav1.DeleteOptions{PropagationPolicy: &policy})
			if err != nil {
				return err
			}
			// TODO(bskiba): This is ugly, change this
			//glog.Infof("Marking node %s for deletion.", node)
			kubemarkProvider.kubemarkCluster.markNodeForDeletion(node)
			return nil
		}
	}

	return fmt.Errorf("Can't delete node %s from nodegroup %s. Node does not exist.", node, nodeGroup)
}

func (kubemarkProvider *Provider) GetNodesForNodegroup(nodeGroup string) ([]string, error) {
	pods := kubemarkProvider.externalCluster.podLister.GetStore().List()
	result := make([]string, 0, len(pods))
	for _, podObj := range pods {
		pod := podObj.(*apiv1.Pod)
		if pod.ObjectMeta.Labels[nodeGroupLabel] == nodeGroup {
			result = append(result, pod.ObjectMeta.Name)
		}
	}
	//glog.Infof("%#v", result)
	return result, nil
}

func (kubemarkProvider *Provider) GetNodeGroupSize(nodeGroup string) (int, error) {
	size := 0
	for _, rcObj := range kubemarkProvider.externalCluster.rcLister.GetStore().List() {
		rc := rcObj.(*apiv1.ReplicationController)
		if rc.Labels[nodeGroupLabel] == nodeGroup {
			size += 1
		}
	}
	return size, nil
}

func (kubemarkProvider *Provider) AddNodesToNodeGroup(nodeGroup string, delta int) error {
	if delta < 0 {
		return fmt.Errorf("Delta has to be positive. Got %d", delta)
	}
	for i := 0; i < delta; i++ {
		if err := kubemarkProvider.addNodeToNodeGroup(nodeGroup); err != nil {
			return err
		}
	}
	return nil
}

func (kubemarkProvider *Provider) RemoveNodesFromNodeGroup(nodeGroup string, delta int) error {
	if delta < 0 {
		return fmt.Errorf("Delta has to be positive. Got %d", delta)
	}
	nodes, err := kubemarkProvider.GetNodesForNodegroup(nodeGroup)
	if err != nil {
		return err
	}
	for i, node := range nodes {
		if i == delta {
			return nil
		}
		if err := kubemarkProvider.DeleteNode(nodeGroup, node); err != nil {
			return err
		}
	}
	return fmt.Errorf("Can't remove %p nodes from %s nodegroup, not enough nodes.")
}

func (kubemarkProvider *Provider) getNodeGroupByName(nodeGroup string) *apiv1.ReplicationController {
	for _, obj := range kubemarkProvider.externalCluster.rcLister.GetStore().List() {
		rc := obj.(*apiv1.ReplicationController)
		if rc.ObjectMeta.Labels[nodeGroupLabel] == nodeGroup {
			return rc
		}
	}
	return nil
}

func (kubemarkProvider *Provider) getReplicationControllerByName(name string) *apiv1.ReplicationController {
	for _, obj := range kubemarkProvider.externalCluster.rcLister.GetStore().List() {
		rc := obj.(*apiv1.ReplicationController)
		if rc.ObjectMeta.Name == name {
			return rc
		}
	}
	return nil
}

var r *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

func RandomString(strlen int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, strlen)
	for i := range result {
		result[i] = chars[r.Intn(len(chars))]
	}
	return string(result)
}

func (kubemarkProvider *Provider) addNodeToNodeGroup(nodeGroup string) error {
	templateCopy, err := api.Scheme.Copy(kubemarkProvider.nodeTemplate)
	if err != nil {
		return err
	}
	node := templateCopy.(*apiv1.ReplicationController)
	node.Name = nodeGroup + "-" + RandomString(6)
	node.Labels = map[string]string{nodeGroupLabel: nodeGroup, "name": node.Name}
	node.Spec.Template.Labels = node.Labels
	_, err = kubemarkProvider.externalCluster.client.CoreV1().ReplicationControllers(node.Namespace).Create(node)
	return err
}

func (kubemarkProvider *Provider) getNodeTemplate() (*apiv1.ReplicationController, error) {
	if hollowNode := kubemarkProvider.getReplicationControllerByName(hollowNodeName); hollowNode != nil {
		nodeTemplate := &apiv1.ReplicationController{
			Spec: apiv1.ReplicationControllerSpec{
				Template: hollowNode.Spec.Template,
			},
		}

		nodeTemplate.Spec.Selector = nil
		nodeTemplate.Namespace = namespaceKubemark
		one := int32(1)
		nodeTemplate.Spec.Replicas = &one

		return nodeTemplate, nil
	}
	return nil, fmt.Errorf("Can't get hollow node template.")
}

func (kubemarkCluster *KubemarkCluster) removeUnneededNodes(oldObj interface{}, newObj interface{}) {
	newNode, ok := newObj.(*apiv1.Node)
	if !ok {
		return
	}
	for _, condition := range newNode.Status.Conditions {
		if condition.Type == apiv1.NodeReady && condition.Status != apiv1.ConditionTrue {
			kubemarkCluster.nodesToDeleteLock.Lock()
			defer kubemarkCluster.nodesToDeleteLock.Unlock()
			if kubemarkCluster.nodesToDelete[newNode.Name] {
				kubemarkCluster.nodesToDelete[newNode.Name] = false
				if err := kubemarkCluster.client.CoreV1().Nodes().Delete(newNode.Name, &metav1.DeleteOptions{}); err != nil {
					glog.Error("Failed to delete node %s from kubemark cluster", newNode.Name)
				}
			}
			return
		}
	}
}

func (kubemarkCluster *KubemarkCluster) markNodeForDeletion(name string) {
	kubemarkCluster.nodesToDeleteLock.Lock()
	defer kubemarkCluster.nodesToDeleteLock.Unlock()
	kubemarkCluster.nodesToDelete[name] = true

}

func newReplicationControllerInformer(kubeClient kube_client.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	rcListWatch := cache.NewListWatchFromClient(kubeClient.CoreV1().RESTClient(), "replicationcontrollers", namespaceKubemark, fields.Everything())
	return cache.NewSharedIndexInformer(rcListWatch, &apiv1.ReplicationController{}, resyncPeriod, nil)
}

func newPodInformer(kubeClient kube_client.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	podListWatch := cache.NewListWatchFromClient(kubeClient.CoreV1().RESTClient(), "pods", namespaceKubemark, fields.Everything())
	return cache.NewSharedIndexInformer(podListWatch, &apiv1.Pod{}, resyncPeriod, nil)
}

func newNodeInformer(kubeClient kube_client.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	nodeListWatch := cache.NewListWatchFromClient(kubeClient.CoreV1().RESTClient(), "nodes", "", fields.Everything())
	informer := cache.NewSharedIndexInformer(nodeListWatch, &apiv1.Node{}, resyncPeriod, nil)
	return informer
}
