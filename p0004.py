d1 = {'one': 1, 'two': 2, 'thee': 3, 'four': 4, 'five': 5}
d2 = {}

for k, v in d1.items():
    if v >= 3:
        d2[k] = v


print(d2)



