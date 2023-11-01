package sqlxcluster

import (
	"database/sql"
	"sync"
)

type DBManager struct {
	mutex       sync.RWMutex
	pools       map[string]DB
	lazyAddFunc func(name string) (DB, error)
}

func (m *DBManager) Add(name string, db DB) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.pools == nil {
		m.pools = make(map[string]DB)
	}
	m.pools[name] = db
}

func (m *DBManager) OnLazyAdd(f func(name string) (DB, error)) {
	m.lazyAddFunc = f
}

func (m *DBManager) Get(name string) (DB, error) {
	m.mutex.RLock()
	db := m.pools[name]
	m.mutex.RUnlock()
	if db != nil {
		return db, nil
	}
	f := m.lazyAddFunc
	if f != nil {
		var err error
		func() {
			m.mutex.Lock()
			defer m.mutex.Unlock()

			if m.pools == nil {
				m.pools = make(map[string]DB)
			}
			db = m.pools[name]
			if db == nil {
				db, err = f(name)
				if err == nil {
					m.pools[name] = db
				}
			}
		}()
		if err != nil {
			return nil, err
		}
	}
	if db != nil {
		return db, nil
	}
	return nil, sql.ErrConnDone
}

func (m *DBManager) MustGet(name string) DB {
	db, err := m.Get(name)
	if err == nil {
		return db
	}
	panic(err)
}

func (m *DBManager) Names() []string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	var ls []string
	for k := range m.pools {
		ls = append(ls, k)
	}
	return ls
}

func (m *DBManager) DBs() []DB {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	var ls []DB
	for _, v := range m.pools {
		ls = append(ls, v)
	}
	return ls
}
