l=[1,1,3,1,2,1,3,3,3,3]
import math
import os
import random
import re
import sys


# Complete the sockMerchant function below.
count=0
l.sort()
print(l)
for i in range(len(l)+9):
    l.remove(l[i])

    if l[i]==l[i+1]:
        l.remove(l[i+1])
        count+=1
print(count)


#
# n = int(input())
# ar=[]
# for i in range(n):
#     p=int(input())
#     ar.append(p)
#






# print(ar)
