import os
import sys
import csv


cl = os.listdir('csv')
print(cl)
for fff in cl:
    if fff[0:-9] == 'CH002_WSThinkRP 123_20220215110455':
        with open(f'CSV\\{fff}', 'r') as of:
            content = of.readlines()
            with open(f'CSV\\test3{fff[-9:]}', 'w') as nf:
                nf.writelines(content)
        print(fff)
