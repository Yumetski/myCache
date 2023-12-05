package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// Hash 定义hash算法，默认为 crc32.ChecksumIEEE
type Hash func(data []byte) uint32

// Map 一致性哈希算法的主数据结构
/**
为了防止数据倾斜，使用虚拟节点扩充了节点的数量
*/
type Map struct {
	hash     Hash           //哈希算法
	replicas int            //虚拟节点倍数
	keys     []int          //哈希环
	hashMap  map[int]string //虚拟节点与真实节点的映射表
}

func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}
	if m.hash != nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Add 添加真实节点的方法
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		//添加虚拟节点
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return ""
	}
	hash := int(m.hash([]byte(key)))
	//顺时针找到第一个匹配的虚拟节点的下表 idx
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	return m.hashMap[m.keys[idx%len(m.keys)]]
}
