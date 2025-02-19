// Oracle基础层
// 主要用于与 Oracle 数据库连接
package toolbase

import (
	"sync"
)
type SafeMap struct {
	//mu sync.RWMutex
	data sync.Map
}

func NewSafeMap() *SafeMap {
	return &SafeMap{}
}
// Put 方法用于添加或更新键值对
func (sm *SafeMap) Put(key string, value interface{}) {
	//sm.mu.Lock()
	//defer sm.mu.Unlock()
	sm.data.Store(key, value)
}
// Get 方法用于获取键对应的值
func (sm *SafeMap) Get(key string) (interface{}, bool) {
	//sm.mu.RLock()
	//defer sm.mu.RUnlock()
	value, ok := sm.data.Load(key)
	return value, ok
}
// Delete 方法用于删除键值对
func (sm *SafeMap) Delete(key string) {
	//sm.mu.Lock()
	//defer sm.mu.Unlock()
	sm.data.Delete(key)
}
// Exists 方法用于判断键是否存在
func (sm *SafeMap) Exists(key string) (interface{}, bool) {
	//sm.mu.RLock()
	//defer sm.mu.RUnlock()
	// Load方法返回value和是否存在的bool值
	value, exists := sm.data.Load(key)
	if !exists {
		// 如果不存在，Load会返回零值和false
		return nil, false
	}
	// 如果存在，返回value和true
	return value, true
}
// EstimateLen 方法用于估算 SafeMapS 中元素的数量（即当前 map 的长度）
func (sm *SafeMap) EstimateLen() int {
	//sm.mu.RLock()
	//defer sm.mu.RUnlock()

	var length int
	sm.data.Range(func(_, _ interface{}) bool {
		length++
		return true
	})

	return length
}
// Range 方法遍历map
func (sm *SafeMap) Range(f func(key interface{}, value interface{}) bool) {
	//sm.mu.RLock()
	//defer sm.mu.RUnlock()
	sm.data.Range(f)
}
//清空集合
func (sm *SafeMap) Clear() {
	//sm.mu.Lock()
	//defer sm.mu.Unlock()
	sm.data.Range(func(key, _ interface{}) bool {
		sm.data.Delete(key)
		return true
	})
}
//您的 Update 方法接受一个键和一个更新函数 updater，该函数接收旧值并返回新值以及一个布尔值指示是否保留键。这意味着 Update 允许在原子操作中对现有键的值进行复杂变换，并可以选择是否保留该键。这种操作比简单的 Set 更为灵活，因为它允许根据旧值计算新值，并且可以选择在某些条件不满足时删除键。
func (sm *SafeMap) Update(key string, updater func(oldValue interface{}) (newValue interface{}, keep bool)) (interface{}, bool) {
	//sm.mu.Lock()
	//defer sm.mu.Unlock()

	oldValue, exists := sm.data.Load(key)
	if !exists {
		return nil, false
	}

	newValue, keep := updater(oldValue)
	if keep {
		sm.data.Store(key, newValue)
	} else {
		sm.data.Delete(key)
	}
	return newValue, keep
}