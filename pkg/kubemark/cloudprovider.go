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
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	informersv1 "k8s.io/client-go/informers/core/v1"
	kubeclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/api"

	"github.com/golang/glog"
)

const (
	namespaceKubemark = "kubemark"
	nodeGroupLabel    = "autoscaling.k8s.io/nodegroup"
)

// Provider is a simplified version of cloud provider for kubemark. It allows
// to add and delete nodes from a kubemark cluster and introduces nodegroups
// by applying labels to the kubemark's hollow-nodes.
type Provider struct {
	nodeTemplate    *apiv1.ReplicationController
	externalCluster externalCluster
	kubemarkCluster kubemarkCluster
}

// externalCluster is used to communicate with the external cluster that hosts
// kubemark, in order to be able to list, create and delete hollow nodes
// by manipulating the replication controllers.
type externalCluster struct {
	rcLister  cache.SharedInformer
	podLister cache.SharedInformer
	client    kubeclient.Interface
}

// kubemarkCluster is used to delete nodes from kubemark cluster once their
// respective replication controllers have been deleted and the nodes have
// become unready. This is to cover for the fact that there is no proper cloud
// provider for kubemark that would care for deleting the nodes.
type kubemarkCluster struct {
	client            kubeclient.Interface
	nodeLister        informersv1.NodeInformer
	nodesToDelete     map[string]bool
	nodesToDeleteLock sync.Mutex
}

// NewProvider creates Provider using the privided clients to talk to external
// and kubemark clusters. It returns an error if it fails to sync data from
// any of the two clusters.
func NewProvider(externalClient kubeclient.Interface, kubemarkClient kubeclient.Interface, stop <-chan struct{}) (*Provider, error) {
	externalInformerFactory := informers.NewSharedInformerFactory(externalClient, 0)
	kubemarkInformerFactory := informers.NewSharedInformerFactory(kubemarkClient, 0)
	manager := &Provider{
		externalCluster: externalCluster{
			rcLister:  externalInformerFactory.InformerFor(&apiv1.ReplicationController{}, newReplicationControllerInformer),
			podLister: externalInformerFactory.InformerFor(&apiv1.Pod{}, newPodInformer),
			client:    externalClient,
		},
		kubemarkCluster: kubemarkCluster{
			nodeLister:        kubemarkInformerFactory.Core().V1().Nodes(),
			client:            kubemarkClient,
			nodesToDelete:     make(map[string]bool),
			nodesToDeleteLock: sync.Mutex{},
		},
	}

	externalInformerFactory.Start(stop)
	for _, synced := range externalInformerFactory.WaitForCacheSync(stop) {
		if !synced {
			return nil, fmt.Errorf("failed to sync data of external cluster")
		}
	}
	kubemarkInformerFactory.Start(stop)
	for _, synced := range kubemarkInformerFactory.WaitForCacheSync(stop) {
		if !synced {
			return nil, fmt.Errorf("failed to sync data of external cluster")
		}
	}
	manager.kubemarkCluster.nodeLister.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: manager.kubemarkCluster.removeUnneededNodes,
	})

	rand.Seed(time.Now().UTC().UnixNano())

	// Get hollow node template from an existing hollow node to be able to create
	// new nodes based on it.
	nodeTemplate, err := manager.getNodeTemplate()
	if err != nil {
		return nil, err
	}
	manager.nodeTemplate = nodeTemplate
	return manager, nil
}

// GetNodeGroupForNode returns the name of the node group to which node belongs.
// Returns an error if node group for node was not found.
func (kubemarkProvider *Provider) GetNodeGroupForNode(node string) (string, error) {
	for _, podObj := range kubemarkProvider.externalCluster.podLister.GetStore().List() {
		pod, ok := podObj.(*apiv1.Pod)
		if !ok {
			continue
		}
		if pod.ObjectMeta.Name == node {
			nodeGroup, ok := pod.ObjectMeta.Labels[nodeGroupLabel]
			if ok {
				return nodeGroup, nil
			}
			return "", fmt.Errorf("can't find nodegroup for node %s. Node exists but does not have the %s label", nodeGroupLabel, node)
		}
	}
	return "", fmt.Errorf("can't find nodegroup for node %s", node)
}

// GetNodesForNodegroup returns list of the nodes in the node group.
func (kubemarkProvider *Provider) GetNodesForNodegroup(nodeGroup string) ([]string, error) {
	pods := kubemarkProvider.externalCluster.podLister.GetStore().List()
	result := make([]string, 0, len(pods))
	for _, podObj := range pods {
		pod, ok := podObj.(*apiv1.Pod)
		if !ok {
			continue
		}
		if pod.ObjectMeta.Labels[nodeGroupLabel] == nodeGroup {
			result = append(result, pod.ObjectMeta.Name)
		}
	}
	return result, nil
}

// GetNodeGroupSize returns the current size for the node group.
func (kubemarkProvider *Provider) GetNodeGroupSize(nodeGroup string) (int, error) {
	size := 0
	for _, rcObj := range kubemarkProvider.externalCluster.rcLister.GetStore().List() {
		rc := rcObj.(*apiv1.ReplicationController)
		if rc.Labels[nodeGroupLabel] == nodeGroup {
			size++
		}
	}
	return size, nil
}

// SetNodeGroupSize changes the size of node group by adding or removing nodes.
func (kubemarkProvider *Provider) SetNodeGroupSize(nodeGroup string, size int) error {
	currSize, err := kubemarkProvider.GetNodeGroupSize(nodeGroup)
	if err != nil {
		return err
	}
	switch delta := currSize - size; {
	case delta < 0:
		return kubemarkProvider.removeNodesFromNodeGroup(nodeGroup, delta)
	case delta > 0:
		return kubemarkProvider.addNodesToNodeGroup(nodeGroup, -delta)
	}
	return nil
}

func (kubemarkProvider *Provider) addNodesToNodeGroup(nodeGroup string, delta int) error {
	if delta < 0 {
		return fmt.Errorf("delta has to be positive. Got %d", delta)
	}
	for i := 0; i < delta; i++ {
		if err := kubemarkProvider.addNodeToNodeGroup(nodeGroup); err != nil {
			return err
		}
	}
	return nil
}

func (kubemarkProvider *Provider) removeNodesFromNodeGroup(nodeGroup string, delta int) error {
	if delta < 0 {
		return fmt.Errorf("delta has to be positive. Got %d", delta)
	}
	nodes, err := kubemarkProvider.GetNodesForNodegroup(nodeGroup)
	if err != nil {
		return err
	}
	for i, node := range nodes {
		if i == delta {
			return nil
		}
		if err := kubemarkProvider.removeNodeFromNodeGroup(nodeGroup, node); err != nil {
			return err
		}
	}
	return fmt.Errorf("can't remove %d nodes from %s nodegroup, not enough nodes", delta, nodeGroup)
}

func (kubemarkProvider *Provider) removeNodeFromNodeGroup(nodeGroup string, node string) error {
	for _, podObj := range kubemarkProvider.externalCluster.podLister.GetStore().List() {
		pod, ok := podObj.(*apiv1.Pod)
		if !ok {
			continue
		}
		if pod.ObjectMeta.Name == node {
			if pod.ObjectMeta.Labels[nodeGroupLabel] != nodeGroup {
				return fmt.Errorf("can't delete node %s from nodegroup %s. Node is not in nodegroup", node, nodeGroup)
			}
			policy := metav1.DeletePropagationForeground
			err := kubemarkProvider.externalCluster.client.CoreV1().ReplicationControllers(namespaceKubemark).Delete(
				pod.ObjectMeta.Labels["name"],
				&metav1.DeleteOptions{PropagationPolicy: &policy})
			if err != nil {
				return err
			}
			glog.Infof("marking node %s for deletion", node)
			// Mark node for deleteion from kubemark cluster.
			// Once it becomes unready after replication controller
			// deletion has been noticed, we will delete it explicitly.
			// This is to cover for the fact that kubemark does not
			// take care of this itself.
			kubemarkProvider.kubemarkCluster.markNodeForDeletion(node)
			return nil
		}
	}

	return fmt.Errorf("can't delete node %s from nodegroup %s. Node does not exist", node, nodeGroup)
}

func (kubemarkProvider *Provider) getNodeGroupByName(nodeGroup string) *apiv1.ReplicationController {
	for _, obj := range kubemarkProvider.externalCluster.rcLister.GetStore().List() {
		rc, ok := obj.(*apiv1.ReplicationController)
		if !ok {
			continue
		}
		if rc.ObjectMeta.Labels[nodeGroupLabel] == nodeGroup {
			return rc
		}
	}
	return nil
}

func (kubemarkProvider *Provider) getReplicationControllerByName(name string) *apiv1.ReplicationController {
	for _, obj := range kubemarkProvider.externalCluster.rcLister.GetStore().List() {
		rc, ok := obj.(*apiv1.ReplicationController)
		if !ok {
			continue
		}
		if rc.ObjectMeta.Name == name {
			return rc
		}
	}
	return nil
}

func (kubemarkProvider *Provider) getNodeNameForPod(podName string) (string, error) {
	for _, obj := range kubemarkProvider.externalCluster.podLister.GetStore().List() {
		pod := obj.(*apiv1.Pod)
		if pod.ObjectMeta.Name == podName {
			return pod.Labels["name"], nil
		}
	}
	return "", fmt.Errorf("pod %s not found", podName)
}

func (kubemarkProvider *Provider) addNodeToNodeGroup(nodeGroup string) error {
	templateCopy, err := api.Scheme.Copy(kubemarkProvider.nodeTemplate)
	if err != nil {
		return err
	}
	node := templateCopy.(*apiv1.ReplicationController)
	node.Name = fmt.Sprintf("%s-%d", nodeGroup, rand.Int63())
	node.Labels = map[string]string{nodeGroupLabel: nodeGroup, "name": node.Name}
	node.Spec.Template.Labels = node.Labels
	_, err = kubemarkProvider.externalCluster.client.CoreV1().ReplicationControllers(node.Namespace).Create(node)
	return err
}

// getNodeTemplate returns the template for hollow node replication controllers
// by looking for an existing hollow node specification. This requires at least
// one kubemark node to be present on startup.
func (kubemarkProvider *Provider) getNodeTemplate() (*apiv1.ReplicationController, error) {
	podName, err := kubemarkProvider.kubemarkCluster.getHollowNodeName()
	if err != nil {
		return nil, err
	}
	hollowNodeName, err := kubemarkProvider.getNodeNameForPod(podName)
	if err != nil {
		return nil, err
	}
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
	return nil, fmt.Errorf("can't get hollow node template")
}

func (kubemarkCluster *kubemarkCluster) getHollowNodeName() (string, error) {
	nodes, err := kubemarkCluster.nodeLister.Lister().List(labels.Everything())
	if err != nil {
		return "", err
	}
	for _, node := range nodes {
		return node.Name, nil
	}
	return "", fmt.Errorf("did not find any hollow nodes in the cluster")
}

func (kubemarkCluster *kubemarkCluster) removeUnneededNodes(oldObj interface{}, newObj interface{}) {
	node, ok := newObj.(*apiv1.Node)
	if !ok {
		return
	}
	for _, condition := range node.Status.Conditions {
		// Delete node if it is in unready state, and it has been
		// explicitly marked for deletion.
		if condition.Type == apiv1.NodeReady && condition.Status != apiv1.ConditionTrue {
			kubemarkCluster.nodesToDeleteLock.Lock()
			defer kubemarkCluster.nodesToDeleteLock.Unlock()
			if kubemarkCluster.nodesToDelete[node.Name] {
				kubemarkCluster.nodesToDelete[node.Name] = false
				if err := kubemarkCluster.client.CoreV1().Nodes().Delete(node.Name, &metav1.DeleteOptions{}); err != nil {
					glog.Errorf("failed to delete node %s from kubemark cluster", node.Name)
				}
			}
			return
		}
	}
}

func (kubemarkCluster *kubemarkCluster) markNodeForDeletion(name string) {
	kubemarkCluster.nodesToDeleteLock.Lock()
	defer kubemarkCluster.nodesToDeleteLock.Unlock()
	kubemarkCluster.nodesToDelete[name] = true
}

func newReplicationControllerInformer(kubeClient kubeclient.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	rcListWatch := cache.NewListWatchFromClient(kubeClient.CoreV1().RESTClient(), "replicationcontrollers", namespaceKubemark, fields.Everything())
	return cache.NewSharedIndexInformer(rcListWatch, &apiv1.ReplicationController{}, resyncPeriod, nil)
}

func newPodInformer(kubeClient kubeclient.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	podListWatch := cache.NewListWatchFromClient(kubeClient.CoreV1().RESTClient(), "pods", namespaceKubemark, fields.Everything())
	return cache.NewSharedIndexInformer(podListWatch, &apiv1.Pod{}, resyncPeriod, nil)
}
