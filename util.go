package searchengine

import "sort"

// sortedKeys 返回 map 的有序键，用于消除 map 遍历的不确定性，
// 保证固定输入下段文件布局与查询结果可复现。
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// cloneInts 返回独立的 int 切片副本。
func cloneInts(s []int) []int {
	if len(s) == 0 {
		return nil
	}
	out := make([]int, len(s))
	copy(out, s)
	return out
}

func cloneI64Map(m map[string]int64) map[string]int64 {
	if m == nil {
		return make(map[string]int64)
	}
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneIntMap(m map[string]int) map[string]int {
	if m == nil {
		return make(map[string]int)
	}
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
