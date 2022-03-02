
import os

li = os.listdir('D:\\labview\\test3\\85p')
datan = li[0].split('-')[0]
print(datan)
count = 1

while count <= 595:
    for c in li:
        with open(f'D:\\labview\\test3\\85p\\{c}', 'r') as csv:
            a = csv.readlines()
            with open(f'D:\\labview\\test3\\595p\\data595-{count:04d}.csv', 'w') as csv2:
                csv2.writelines(a)
                count += 1
