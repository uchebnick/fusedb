package oneleafdb

import "fmt"

func DBKey(i int) []byte {
	return []byte(fmt.Sprintf("key:%016d", i))
}
