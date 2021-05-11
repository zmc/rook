/*
Copyright 2016 The Rook Authors. All rights reserved.

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

package mon

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/rook/rook/pkg/operator/ceph/config"
	"github.com/rook/rook/pkg/operator/k8sutil"

	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	"github.com/rook/rook/pkg/clusterd"
	"github.com/rook/rook/pkg/daemon/ceph/client"
	cephclient "github.com/rook/rook/pkg/daemon/ceph/client"
	clienttest "github.com/rook/rook/pkg/daemon/ceph/client/test"
	cephver "github.com/rook/rook/pkg/operator/ceph/version"
	"github.com/rook/rook/pkg/operator/test"
	exectest "github.com/rook/rook/pkg/util/exec/test"
	"github.com/stretchr/testify/assert"
	"github.com/tevino/abool"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// generate a standard mon config from a mon id w/ default port and IP 2.4.6.{1,2,3,...}
// support mon ID as new ["a", "b", etc.] form or as legacy ["mon0", "mon1", etc.] form
func testGenMonConfig(monID string) *monConfig {
	var moniker string
	var index int
	var err error
	if strings.HasPrefix(monID, "mon") { // is legacy mon name
		moniker = monID                                                 // keep legacy "mon#" name
		index, err = strconv.Atoi(strings.Replace(monID, "mon", "", 1)) // get # off end of mon#
	} else {
		moniker = "mon-" + monID
		index, err = k8sutil.NameToIndex(monID)
	}
	if err != nil {
		panic(err)
	}
	return &monConfig{
		ResourceName: "rook-ceph-" + moniker, // rook-ceph-mon-A or rook-ceph-mon#
		DaemonName:   monID,                  // A or mon#
		Port:         DefaultMsgr1Port,
		PublicIP:     fmt.Sprintf("2.4.6.%d", index+1),
		// dataDirHostPath assumed to be /var/lib/rook
		DataPathMap: config.NewStatefulDaemonDataPathMap(
			"/var/lib/rook", dataDirRelativeHostPath(monID), config.MonType, monID, "rook-ceph"),
	}
}

func newTestStartCluster(t *testing.T, namespace string) (*clusterd.Context, error) {
	monResponse := func() (string, error) {
		return clienttest.MonInQuorumResponseMany(3), nil
	}
	return newTestStartClusterWithQuorumResponse(t, namespace, monResponse)
}

func newTestStartClusterWithQuorumResponse(t *testing.T, namespace string, monResponse func() (string, error)) (*clusterd.Context, error) {
	clientset := test.New(t, 3)
	configDir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(configDir)
	executor := &exectest.MockExecutor{
		MockExecuteCommandWithOutput: func(command string, args ...string) (string, error) {
			if strings.Contains(command, "ceph-authtool") {
				err := clienttest.CreateConfigDir(path.Join(configDir, namespace))
				return "", errors.Wrap(err, "failed testing of start cluster without quorum response")
			}
			return "", nil
		},
		MockExecuteCommandWithOutputFile: func(command string, outFileArg string, args ...string) (string, error) {
			// mock quorum health check because a second `Start()` triggers a health check
			return monResponse()
		},
	}
	return &clusterd.Context{
		Clientset:                  clientset,
		Executor:                   executor,
		ConfigDir:                  configDir,
		RequestCancelOrchestration: abool.New(),
	}, nil
}

func newCluster(context *clusterd.Context, namespace string, allowMultiplePerNode bool, resources v1.ResourceRequirements) *Cluster {
	return &Cluster{
		ClusterInfo: nil,
		context:     context,
		Namespace:   namespace,
		rookVersion: "myversion",
		spec: cephv1.ClusterSpec{
			Mon: cephv1.MonSpec{
				Count:                3,
				AllowMultiplePerNode: allowMultiplePerNode,
			},
			Resources: map[string]v1.ResourceRequirements{"mon": resources},
		},
		maxMonID:            -1,
		waitForStart:        false,
		monPodRetryInterval: 10 * time.Millisecond,
		monPodTimeout:       1 * time.Second,
		monTimeoutList:      map[string]time.Time{},
		mapping: &Mapping{
			Node: map[string]*NodeInfo{},
		},
		ownerRef: metav1.OwnerReference{},
	}
}

// setCommonMonProperties is a convenience helper for setting common test properties
func setCommonMonProperties(c *Cluster, currentMons int, mon cephv1.MonSpec, rookVersion string) {
	c.ClusterInfo = clienttest.CreateTestClusterInfo(currentMons)
	c.spec.Mon.Count = mon.Count
	c.spec.Mon.AllowMultiplePerNode = mon.AllowMultiplePerNode
	c.rookVersion = rookVersion
}

func TestResourceName(t *testing.T) {
	assert.Equal(t, "rook-ceph-mon-a", resourceName("rook-ceph-mon-a"))
	assert.Equal(t, "rook-ceph-mon123", resourceName("rook-ceph-mon123"))
	assert.Equal(t, "rook-ceph-mon-b", resourceName("b"))
}

func TestStartMonPods(t *testing.T) {

	namespace := "ns"
	context, err := newTestStartCluster(t, namespace)
	assert.Nil(t, err)
	c := newCluster(context, namespace, true, v1.ResourceRequirements{})
	c.ClusterInfo = clienttest.CreateTestClusterInfo(1)

	// start a basic cluster
	_, err = c.Start(c.ClusterInfo, c.rookVersion, cephver.Nautilus, c.spec)
	assert.Nil(t, err)

	validateStart(t, c)

	// starting again should be a no-op, but still results in an error
	_, err = c.Start(c.ClusterInfo, c.rookVersion, cephver.Nautilus, c.spec)
	assert.Nil(t, err)

	validateStart(t, c)
}

func TestOperatorRestart(t *testing.T) {

	namespace := "ns"
	context, err := newTestStartCluster(t, namespace)
	assert.Nil(t, err)
	c := newCluster(context, namespace, true, v1.ResourceRequirements{})
	c.ClusterInfo = clienttest.CreateTestClusterInfo(1)

	// start a basic cluster
	info, err := c.Start(c.ClusterInfo, c.rookVersion, cephver.Nautilus, c.spec)
	assert.Nil(t, err)
	assert.True(t, info.IsInitialized(true))

	validateStart(t, c)

	c = newCluster(context, namespace, true, v1.ResourceRequirements{})
	c.ClusterInfo = clienttest.CreateTestClusterInfo(1)

	// starting again should be a no-op, but will not result in an error
	info, err = c.Start(c.ClusterInfo, c.rookVersion, cephver.Nautilus, c.spec)
	assert.Nil(t, err)
	assert.True(t, info.IsInitialized(true))

	validateStart(t, c)
}

// safety check that if hostNetwork is used no changes occur on an operator restart
func TestOperatorRestartHostNetwork(t *testing.T) {

	namespace := "ns"
	context, err := newTestStartCluster(t, namespace)
	assert.Nil(t, err)

	// cluster without host networking
	c := newCluster(context, namespace, false, v1.ResourceRequirements{})
	c.ClusterInfo = clienttest.CreateTestClusterInfo(1)

	// start a basic cluster
	info, err := c.Start(c.ClusterInfo, c.rookVersion, cephver.Nautilus, c.spec)
	assert.Nil(t, err)
	assert.True(t, info.IsInitialized(true))

	validateStart(t, c)

	// cluster with host networking
	c = newCluster(context, namespace, false, v1.ResourceRequirements{})
	c.spec.Network.HostNetwork = true
	c.ClusterInfo = clienttest.CreateTestClusterInfo(1)

	// starting again should be a no-op, but still results in an error
	info, err = c.Start(c.ClusterInfo, c.rookVersion, cephver.Nautilus, c.spec)
	assert.Nil(t, err)
	assert.True(t, info.IsInitialized(true), info)

	validateStart(t, c)
}

func validateStart(t *testing.T, c *Cluster) {
	s, err := c.context.Clientset.CoreV1().Secrets(c.Namespace).Get(AppName, metav1.GetOptions{})
	assert.NoError(t, err) // there shouldn't be an error due the secret existing
	assert.Equal(t, 4, len(s.Data))

	// there is only one pod created. the other two won't be created since the first one doesn't start
	_, err = c.context.Clientset.AppsV1().Deployments(c.Namespace).Get("rook-ceph-mon-a", metav1.GetOptions{})
	assert.Nil(t, err)
}

func TestPersistMons(t *testing.T) {
	clientset := test.New(t, 1)
	c := New(&clusterd.Context{Clientset: clientset}, "ns", cephv1.ClusterSpec{}, metav1.OwnerReference{}, &sync.Mutex{})
	setCommonMonProperties(c, 1, cephv1.MonSpec{Count: 3, AllowMultiplePerNode: true}, "myversion")

	// Persist mon a
	err := c.persistExpectedMonDaemons()
	assert.NoError(t, err)

	cm, err := c.context.Clientset.CoreV1().ConfigMaps(c.Namespace).Get(EndpointConfigMapName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, "a=1.2.3.1:6789", cm.Data[EndpointDataKey])

	// Persist mon b, and remove mon a for simply testing the configmap is updated
	c.ClusterInfo.Monitors["b"] = &cephclient.MonInfo{Name: "b", Endpoint: "4.5.6.7:3300"}
	delete(c.ClusterInfo.Monitors, "a")
	err = c.persistExpectedMonDaemons()
	assert.NoError(t, err)

	cm, err = c.context.Clientset.CoreV1().ConfigMaps(c.Namespace).Get(EndpointConfigMapName, metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, "b=4.5.6.7:3300", cm.Data[EndpointDataKey])
}

func TestSaveMonEndpoints(t *testing.T) {
	clientset := test.New(t, 1)
	configDir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(configDir)
	c := New(&clusterd.Context{Clientset: clientset, ConfigDir: configDir}, "ns", cephv1.ClusterSpec{}, metav1.OwnerReference{}, &sync.Mutex{})
	setCommonMonProperties(c, 1, cephv1.MonSpec{Count: 3, AllowMultiplePerNode: true}, "myversion")

	// create the initial config map
	err := c.saveMonConfig()
	assert.Nil(t, err)

	cm, err := c.context.Clientset.CoreV1().ConfigMaps(c.Namespace).Get(EndpointConfigMapName, metav1.GetOptions{})
	assert.Nil(t, err)
	assert.Equal(t, "a=1.2.3.1:6789", cm.Data[EndpointDataKey])
	assert.Equal(t, `{"node":{}}`, cm.Data[MappingKey])
	assert.Equal(t, "-1", cm.Data[MaxMonIDKey])

	// update the config map
	c.ClusterInfo.Monitors["a"].Endpoint = "2.3.4.5:6789"
	c.maxMonID = 2
	c.mapping.Node["a"] = &NodeInfo{
		Name:     "node0",
		Address:  "1.1.1.1",
		Hostname: "myhost",
	}
	err = c.saveMonConfig()
	assert.Nil(t, err)

	cm, err = c.context.Clientset.CoreV1().ConfigMaps(c.Namespace).Get(EndpointConfigMapName, metav1.GetOptions{})
	assert.Nil(t, err)
	assert.Equal(t, "a=2.3.4.5:6789", cm.Data[EndpointDataKey])
	assert.Equal(t, `{"node":{"a":{"Name":"node0","Hostname":"myhost","Address":"1.1.1.1"}}}`, cm.Data[MappingKey])
	assert.Equal(t, "-1", cm.Data[MaxMonIDKey])

	// Update the maxMonID to some random value
	cm.Data[MaxMonIDKey] = "23"
	_, err = c.context.Clientset.CoreV1().ConfigMaps(c.Namespace).Update(cm)
	assert.Nil(t, err)
	// Confirm the maxMonId will be persisted and not updated to anything else.
	// The value is only expected to be set directly to the configmap when a mon deployment is started.
	err = c.saveMonConfig()
	assert.Nil(t, err)
	cm, err = c.context.Clientset.CoreV1().ConfigMaps(c.Namespace).Get(EndpointConfigMapName, metav1.GetOptions{})
	assert.Nil(t, err)
	assert.Equal(t, "23", cm.Data[MaxMonIDKey])
}

func TestMaxMonID(t *testing.T) {
	clientset := test.New(t, 1)
	configDir, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(configDir)
	c := New(&clusterd.Context{Clientset: clientset, ConfigDir: configDir}, "ns", cephv1.ClusterSpec{}, metav1.OwnerReference{}, &sync.Mutex{})

	// when the configmap is not found, the maxMonID is -1
	maxMonID, err := c.getStoredMaxMonID()
	assert.Nil(t, err)
	assert.Equal(t, "-1", maxMonID)

	// initialize the configmap
	setCommonMonProperties(c, 1, cephv1.MonSpec{Count: 3, AllowMultiplePerNode: true}, "myversion")
	err = c.saveMonConfig()
	assert.Nil(t, err)

	// invalid mon names won't update the maxMonID
	err = c.commitMaxMonID("bad-id")
	assert.Error(t, err)

	// starting a mon deployment will set the maxMonID
	err = c.commitMaxMonID("a")
	assert.Nil(t, err)
	maxMonID, err = c.getStoredMaxMonID()
	assert.Nil(t, err)
	assert.Equal(t, "0", maxMonID)

	// set to a higher id
	err = c.commitMaxMonID("d")
	assert.Nil(t, err)
	maxMonID, err = c.getStoredMaxMonID()
	assert.Nil(t, err)
	assert.Equal(t, "3", maxMonID)

	// setting to an id lower than the max will not update it
	err = c.commitMaxMonID("c")
	assert.Nil(t, err)
	maxMonID, err = c.getStoredMaxMonID()
	assert.Nil(t, err)
	assert.Equal(t, "3", maxMonID)
}

func TestMonInQuorum(t *testing.T) {
	entry := client.MonMapEntry{Name: "foo", Rank: 23}
	quorum := []int{}
	// Nothing in quorum
	assert.False(t, monInQuorum(entry, quorum))

	// One or more members in quorum
	quorum = []int{23}
	assert.True(t, monInQuorum(entry, quorum))
	quorum = []int{5, 6, 7, 23, 8}
	assert.True(t, monInQuorum(entry, quorum))

	// Not in quorum
	entry.Rank = 1
	assert.False(t, monInQuorum(entry, quorum))
}

func TestNameToIndex(t *testing.T) {
	// invalid
	id, err := fullNameToIndex("rook-ceph-monitor0")
	assert.NotNil(t, err)
	assert.Equal(t, -1, id)
	id, err = fullNameToIndex("rook-ceph-mon123")
	assert.NotNil(t, err)
	assert.Equal(t, -1, id)

	// valid
	id, err = fullNameToIndex("b")
	assert.Nil(t, err)
	assert.Equal(t, 1, id)
	id, err = fullNameToIndex("m")
	assert.Nil(t, err)
	assert.Equal(t, 12, id)
	id, err = fullNameToIndex("rook-ceph-mon-a")
	assert.Nil(t, err)
	assert.Equal(t, 0, id)
}

func TestWaitForQuorum(t *testing.T) {
	namespace := "ns"
	quorumChecks := 0
	quorumResponse := func() (string, error) {
		mons := map[string]*client.MonInfo{
			"a": {},
		}
		quorumChecks++
		if quorumChecks == 1 {
			// return an error the first time while we're waiting for the mon to join quorum
			return "", errors.New("test error")
		}
		// a successful response indicates that we have quorum, even if we didn't check which specific mons were in quorum
		return clienttest.MonInQuorumResponseFromMons(mons), nil
	}
	context, err := newTestStartClusterWithQuorumResponse(t, namespace, quorumResponse)
	requireAllInQuorum := false
	expectedMons := []string{"a"}
	clusterInfo := &client.ClusterInfo{Namespace: namespace}
	err = waitForQuorumWithMons(context, clusterInfo, expectedMons, 0, requireAllInQuorum)
	assert.Nil(t, err)
}

func TestMonFoundInQuorum(t *testing.T) {
	response := client.MonStatusResponse{}

	// "a" is in quorum
	response.Quorum = []int{0}
	response.MonMap.Mons = []client.MonMapEntry{
		{Name: "a", Rank: 0},
		{Name: "b", Rank: 1},
		{Name: "c", Rank: 2},
	}
	assert.True(t, monFoundInQuorum("a", response))
	assert.False(t, monFoundInQuorum("b", response))
	assert.False(t, monFoundInQuorum("c", response))

	// b and c also in quorum, but not d
	response.Quorum = []int{0, 1, 2}
	assert.True(t, monFoundInQuorum("a", response))
	assert.True(t, monFoundInQuorum("b", response))
	assert.True(t, monFoundInQuorum("c", response))
	assert.False(t, monFoundInQuorum("d", response))
}
