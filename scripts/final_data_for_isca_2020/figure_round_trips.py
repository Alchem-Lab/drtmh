import matplotlib
matplotlib.use("pdf")

import numpy as np
import matplotlib.pyplot as plt

# data to plot
xticks = ('nowait', 'waitdie', 'occ', 'mvcc', 'sundial')
n_groups = len(xticks)
rpc = (3333325, 3435806, 3268506, 2499047, 2699445)
onesided = (4102212, 4007310, 3481180, 4312138, 3638650)

colors=['b','g','r','c','m','y','k','grey']
patterns = [ "","/", "/" , "\\" , "|" , "-" , "+" , "x", "o", "O", ".", "*" ]

# plt.figure(figsize=(24,4))
# create plot
fig, ax = plt.subplots(figsize=(8,2))
index = np.arange(n_groups)
bar_width = 0.3
opacity = 0.8

rects1 = plt.bar(index, rpc, bar_width,
alpha=opacity,
color='b',
label='rpc')

rects2 = plt.bar(index + bar_width, onesided, bar_width,
alpha=opacity,
color='g',
hatch='/',
label='onesided')

#plt.xlabel('')
# plt.ylabel('')
# plt.title('# Network Round Trips')
plt.xticks(index + bar_width/2, xticks, fontsize=16)
plt.legend()

# plt.title("Operation latency in Sundial running YCSB on gorgon(8 thread 10 cor)")
# plt.legend(fontsize=16, bbox_to_anchor=(-2.2, -0.9, 1, .06), loc=3, ncol=2,  borderaxespad=0.)
plt.savefig('network_trips' + '.pdf', bbox_inches='tight')
