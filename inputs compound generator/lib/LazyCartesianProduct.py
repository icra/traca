import math


class LazyCartesianProduct:
    def __init__(self, sets):
        self.sets = sets
        self.divs = []
        self.mods = []
        self.maxSize = 1
        self.precompute()

    def precompute(self):
        for i in self.sets:
            self.maxSize = self.maxSize * len(i)
        length = len(self.sets)
        factor = 1
        for i in range((length - 1), -1, -1):
            items = len(self.sets[i])
            self.divs.insert(0, factor)
            self.mods.insert(0, items)
            factor = factor * items

    def entryAt(self, n):
        length = len(self.sets)
        if n < 0 or n >= self.maxSize:
            raise IndexError
        combination = []
        for i in range(0, length):
            combination.append(self.sets[i][int(math.floor(n / self.divs[i])) % self.mods[i]])
        return combination
