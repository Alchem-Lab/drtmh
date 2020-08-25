def set_sum(nums):
    if len(nums) == 1:
        return nums[:] + [0]
    set1 = set_sum(nums[1:])
    set2 = [nums[0] + x for x in set1]
    return set1 + set2

import sys
if len(sys.argv) > 1:
    print(' '.join([str(y) for y in set_sum([int(n) for n in sys.argv[1:]])]))
