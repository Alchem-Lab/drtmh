import matplotlib
matplotlib.use("pdf")

import numpy as np
import matplotlib.pyplot as plt

# data to plot
xticks = ('nowait', 'waitdie', 'occ', 'mvcc', 'sundial')
n_groups = len(xticks)
series = ('Read', 'Lock', 'Release', 'Commit', 'Renew')
colors=['b','g','r','c','m','y','k','grey']
patterns = [ "","/", "/" , "\\" , "|" , "-" , "+" , "x", "o", "O", ".", "*" ]

plt.figure(figsize=(24,12))

rpc_data = {
        'Read':[0, 0, 41.03, 9.84, 7.72],
        'Lock':[8.21, 9.82, 5.22, 9.68, 7.54],
        'Release':[7.91, 16.01, 15.61, 4.85, 7.13],
        'Commit':[7.32, 7.83, 36.63, 7.8, 6.76],
        'Renew':[0,0,0,0,7.48]
        }
# create plot for rpc
ax = plt.subplot(2,1,1)
index = np.arange(n_groups)
bar_width = 0.15
opacity = 0.8

for i in range(len(series)):
    rects1 = plt.bar(index + i*bar_width, rpc_data[series[i]], bar_width,
    alpha=opacity,
    color=colors[i],
    hatch=patterns[i],
    label=series[i])

plt.ylabel('Latency(ms)', fontsize=32) 
plt.yticks(fontsize=24) 
plt.xticks([], [])
# plt.xticks(index + bar_width*(len(series)-1)/2, xticks)
# plt.title('# Network Round Trips')
plt.legend(fontsize=26)

onesided_data = {
        'Read':[2.45, 2.43, 2.43, 6.94, 4.68],
        'Lock':[3.24, 3.25, 2.92, 7.93, 3.23],
        'Release':[0.61, 0.61, 1.31, 0.78, 0.46],
        'Commit':[2.12, 2.12, 4.08, 2.17, 2.12],
        'Renew':[0,0,0,0,3.93]
        }
# create plot for onesided
ax = plt.subplot(2,1,2)
index = np.arange(n_groups)
bar_width = 0.15
opacity = 0.8

for i in range(len(series)):
    rects1 = plt.bar(index + i*bar_width, onesided_data[series[i]], bar_width,
    alpha=opacity,
    color=colors[i],
    hatch=patterns[i],
    label=series[i])

plt.ylabel('Latency(ms)', fontsize=32) 
plt.yticks(fontsize=24) 
plt.xticks(index + bar_width*(len(series)-1)/2, xticks, fontsize=32)
# plt.title('# Network Round Trips')
plt.legend(fontsize=32)

plt.savefig('lat_breakdown' + '.pdf', bbox_inches='tight')
