"""Fixed-capacity exact binary64 moments, including subnormals and eviction.

Ordinary floating squares underflow for representable tiny inputs and lose small
terms after large-outlier eviction. Fixed point units of 2**-1074 replace the
compensated accumulator: at most 2107 bits for S1 and 4205 bits for S2 (H<=300).
Integer updates are exact; no scans, sorting, diagnostic recovery or epsilon.
Time O(N), space O(H), with binary64 precision/range fixed.
"""

from collections import deque
import math


class Moments:
    def __init__(self, capacity):
        self.capacity = capacity
        self.queue = deque()
        self.count = self.positive = 0
        self.s1 = self.s2 = 0

    @staticmethod
    def units(x):
        a, b = x.as_integer_ratio()
        return a << (1074 - (b.bit_length() - 1))

    def append(self, value):
        if len(self.queue) == self.capacity:
            old = self.queue.popleft()
            if old is not None:
                self.count -= 1
                self.positive -= old > 0
                self.s1 -= old
                self.s2 -= old * old
        x = None
        if value is not None and math.isfinite(value):
            if value < 0:
                raise ValueError("negative movement")
            x = self.units(float(value))
            self.count += 1
            self.positive += x > 0
            self.s1 += x
            self.s2 += x * x
        self.queue.append(x)
        if not self.positive:
            if self.s1 or self.s2:
                raise ValueError("nonzero moments with zero positive count")

    def mean(self):
        return self.s1 / (self.count * (1 << 1074)) if self.count else math.nan

    def participation(self):
        if not self.positive:
            return math.nan
        if self.s1 <= 0 or self.s2 <= 0:
            raise ValueError("invalid exact moments")
        value = (self.s1 * self.s1) / (self.count * self.s2)
        if not math.isfinite(value) or not 1 / self.count <= value <= 1:
            raise ValueError("participation arithmetic/bound failure")
        return value
