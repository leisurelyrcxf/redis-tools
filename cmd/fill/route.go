package main

import (
	"bytes"
	"hash/crc32"
)

func Hash(key []byte) uint32 {
	const (
		TagBeg = '{'
		TagEnd = '}'
	)
	if beg := bytes.IndexByte(key, TagBeg); beg >= 0 {
		if end := bytes.IndexByte(key[beg+1:], TagEnd); end >= 0 {
			key = key[beg+1 : beg+1+end]
		}
	}
	return crc32.ChecksumIEEE(key)
}

func Dispatch(hkey string, maxSlotNum int) (slot int) {
	return int(Hash([]byte(hkey))) % maxSlotNum
}

func ContainsKey(slots []int, hkey string, maxSlotNum int) bool {
	if maxSlotNum == 0 {
		return true
	}
	slot := Dispatch(hkey, maxSlotNum)
	for _, s := range slots {
		if s == slot {
			return true
		}
	}
	return false
}
