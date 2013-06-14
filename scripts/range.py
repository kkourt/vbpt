
def range_leq(r1, r2):
	(r1_start, r1_len) = r1
	(r2_start, r2_len) = r2
	if r1_start > r2_start:
		return False
	x = r2_start - r1_start
	if x > r1_len:
		return False
	if (r1_len-x) < r2_len:
		return False
	return True

