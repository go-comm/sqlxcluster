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

func (m *ClusterDBManager) Add(name string, db *ClusterDB) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.clusters[name] = db
}

func (m *ClusterDBManager) Get(name string) *ClusterDB {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.clusters[name]
}
