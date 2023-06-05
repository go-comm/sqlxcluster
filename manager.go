package sqlxcluster

import (
	"sync"
)

func NewClusterDBManager() *ClusterDBManager {
	m := &ClusterDBManager{clusters: map[string]*ClusterDB{}}
	return m
}

type ClusterDBManager struct {
	mutex    sync.RWMutex
	clusters map[string]*ClusterDB
}

func (m *ClusterDBManager) Add(db *ClusterDB) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.clusters[db.Name()] = db
}

func (m *ClusterDBManager) Get(name string) *ClusterDB {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.clusters[name]
}

func (m *ClusterDBManager) Names() []string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	var ls []string
	for k := range m.clusters {
		ls = append(ls, k)
	}
	return ls
}

func (m *ClusterDBManager) DBs() []*ClusterDB {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	var ls []*ClusterDB
	for _, v := range m.clusters {
		ls = append(ls, v)
	}
	return ls
}
