package cache

// Sixpack compression algorithm — ported from the C implementation.
// Converts 8-bit strings into 6-bit strings using a 64-character set
// commonly used for KV store keys:
//
//	-.0123456789:ABCDEFGHIJKLMNOPRSTUVWXY_abcdefghijklmnopqrstuvwxy
//
// Note: "Q", "Z", "z" are not in the set.
// Achieves ~25% space savings on typical cache keys.

var toSix = [256]byte{
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0-15
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 16-31
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 0, // 32-47
	3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0, 0, 0, 0, 0, // 48-63
	0, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, // 64-79
	29, 0, 30, 31, 32, 33, 34, 35, 36, 37, 0, 0, 0, 0, 0, 38, // 80-95
	0, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, // 96-111
	54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 0, 0, 0, 0, 0, 0, // 112-127
}

var fromSix = [64]byte{
	0, '-', '.', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', ':',
	'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N',
	'O', 'P', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', '_', 'a', 'b', 'c',
	'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q',
	'r', 's', 't', 'u', 'v', 'w', 'x', 'y',
}

// Sixpack compresses data using 6-bit encoding.
// Returns nil if the data contains characters outside the sixpack character set.
// Max input length is 128 bytes (matching C implementation).
func Sixpack(data []byte) []byte {
	if len(data) == 0 || len(data) > 128 {
		return nil
	}
	// Worst case: ceil(len*6/8) = ceil(len*3/4)
	dst := make([]byte, 0, (len(data)*3+3)/4)
	j := 0
	for i, b := range data {
		k6v := toSix[b]
		if k6v == 0 {
			return nil
		}
		switch i % 4 {
		case 0:
			dst = append(dst, k6v<<2)
			j++
		case 1:
			dst[j-1] |= k6v >> 4
			dst = append(dst, k6v<<4)
			j++
		case 2:
			dst[j-1] |= k6v >> 2
			dst = append(dst, k6v<<6)
			j++
		case 3:
			dst[j-1] |= k6v
		}
	}
	return dst[:j]
}

// Unsixpack decompresses sixpack-encoded data back to the original string.
func Unsixpack(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	// Each 3 bytes of packed data holds 4 characters
	dst := make([]byte, 0, len(data)*4/3+2)
	k := 0
	for i := 0; i < len(data); i++ {
		switch k {
		case 0:
			dst = append(dst, fromSix[data[i]>>2])
			k++
		case 1:
			dst = append(dst, fromSix[((data[i-1]<<4)|(data[i]>>4))&63])
			k++
		default:
			dst = append(dst, fromSix[((data[i-1]<<2)|(data[i]>>6))&63])
			dst = append(dst, fromSix[data[i]&63])
			k = 0
		}
	}
	// Remove trailing null if present (matches C behavior)
	if len(dst) > 0 && dst[len(dst)-1] == 0 {
		dst = dst[:len(dst)-1]
	}
	return dst
}
