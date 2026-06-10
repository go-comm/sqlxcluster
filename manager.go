package sqlxcluster

import (
	"database/sql"
	"sync"
)

type DBManager struct {
	mu    sync.RWMutex
	pools map[string]DB
	New   func(name string) (DB, error)
}

func (m *DBManager) init() {
	if m.pools == nil {
		m.pools = make(map[string]DB)
	}
}

func (m *DBManager) Add(name string, db DB) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.init()
	m.pools[name] = db
}

func (m *DBManager) Remove(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pools, name)
}

func (m *DBManager) Get(name string) (DB, error) {
	m.mu.RLock()
	db := m.pools[name]
	m.mu.RUnlock()
	if db != nil {
		return db, nil
	}
	return m.getOrCreate(name)
}

func (m *DBManager) getOrCreate(name string) (DB, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.init()

	if db := m.pools[name]; db != nil {
		return db, nil
	}

	f := m.New
	if f == nil {
		return nil, sql.ErrConnDone
	}

	db, err := f(name)
	if err != nil {
		return nil, err
	}
	m.pools[name] = db
	return db, nil
}

func (m *DBManager) MustGet(name string) DB {
	db, err := m.Get(name)
	if err == nil {
		return db
	}
	panic(err)
}

func (m *DBManager) Names() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.pools == nil {
		return nil
	}
	ls := make([]string, 0, len(m.pools))
	for k := range m.pools {
		ls = append(ls, k)
	}
	return ls
}

func (m *DBManager) DBs() []DB {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.pools == nil {
		return nil
	}
	ls := make([]DB, 0, len(m.pools))
	for _, v := range m.pools {
		ls = append(ls, v)
	}
	return ls
}

func (m *DBManager) Range(fn func(name string, db DB) bool) {
	m.mu.RLock()
	if m.pools == nil {
		m.mu.RUnlock()
		return
	}

	pools := make([]struct {
		name string
		db   DB
	}, 0, len(m.pools))
	for k, v := range m.pools {
		pools = append(pools, struct {
			name string
			db   DB
		}{k, v})
	}
	m.mu.RUnlock()

	for _, p := range pools {
		if !fn(p.name, p.db) {
			return
		}
	}
}

func (m *DBManager) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.pools)
}

func (m *DBManager) Close(name string) error {
	m.mu.Lock()
	db := m.pools[name]
	delete(m.pools, name)
	m.mu.Unlock()
	if db != nil {
		return db.Close()
	}
	return nil
}

func (m *DBManager) CloseAll() []error {
	m.mu.Lock()
	var dbs []DB
	for _, v := range m.pools {
		dbs = append(dbs, v)
	}
	m.pools = make(map[string]DB)
	m.mu.Unlock()

	var errs []error
	for _, db := range dbs {
		if err := db.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}
