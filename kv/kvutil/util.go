package kvutil

// NextPrefix returns a prefix that is lexicographically larger than the input prefix
func NextPrefix(prefix []byte) []byte {
	buf := make([]byte, len(prefix))
	copy(buf, prefix)
	var i int
	for i = len(prefix) - 1; i >= 0; i-- {
		buf[i]++
		if buf[i] != 0 {
			break
		}
	}
	if i == -1 {
		buf = make([]byte, 0)
	}
	return buf
}
