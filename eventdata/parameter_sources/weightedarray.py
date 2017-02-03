import json
import random
import math
import gzip

class WeightedArray:
    def __init__(self, json_file):
        with gzip.open(json_file, 'rt') as data_file:    
            item_list = json.load(data_file)

        self._items = []
        self._totals = []
        self._sum = 0

        for item in item_list:
            self._sum += item[0]
            self._items.append(item[1])
            self._totals.append(self._sum)

    def get_random(self):
        return self._items[self.__random_index()]

    def __random_index(self):
        minimumIndex = 0
        maximumIndex = len(self._totals)
        total = 0

        random.seed()
        rand = random.random() * self._sum

        while maximumIndex >= minimumIndex:
            middleIndex = (maximumIndex + minimumIndex) / 2

            if middleIndex < 0:
                middleIndex = 0
            else:
                middleIndex = math.floor(middleIndex)

            total = self._totals[middleIndex]

            if rand == total:
                middleIndex += 1
                break
            elif rand < total:
                if middleIndex > 0 and rand > self._totals[middleIndex - 1]:
                    break

                maximumIndex = middleIndex - 1

            else:
                minimumIndex = middleIndex + 1

        return middleIndex
