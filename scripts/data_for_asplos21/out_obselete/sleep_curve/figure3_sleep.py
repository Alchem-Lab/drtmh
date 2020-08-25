# -*- coding: utf-8 -*-
import matplotlib
matplotlib.use('pdf')
matplotlib.rc('pdf', fonttype=42)

import matplotlib.pyplot as plt
import numpy as np
import sys,os
import subprocess
versions = ['rpc','onesided']
# versions = ['rpc','onesided', 'hybrid']
colors=['b','g','r','c','y','m','k','grey']
algs = ['nowait', 'waitdie', 'occ', 'mvcc', 'sundial']
out_path = sys.argv[1]
apps = ['bank', 'ycsb', 'tpcc']
versions = ['rpc','onesided']
version_format = {'rpc':'RPC', 'onesided':'one-sided'}

markers = ['x', '1', '.', '^', 'd', 's']
patterns = [ "","/", "/" , "\\" , "|" , "-" , "+" , "x", "o", "O", ".", "*" ]
datapointnum = 9

def get_tput(filedir):
    tres = 0.0
    res = ""
    if os.path.isfile(filedir):
        res = subprocess.check_output(('awk', '-f', 'tput.awk', filedir))
    else:
        print("no file: " + filedir)
    if res.strip() != "":
        tres = float(res.strip())
    else:
        print("not num: " + filedir)
    return tres

# versions = ['rpc','onesided', 'hybrid']
plt.figure(figsize=(10,3))
for i, version in enumerate(versions):
    ax = plt.subplot(1,2,i + 1)
    num = [[],[],[],[],[],[]]
    x_arr = []
    real_x = []
    for idx in range(datapointnum):
        x_arr.append(2**idx)
        real_x.append(idx + 1)
    for which, alg in enumerate(algs):
        for x in range(datapointnum):
            fname = out_path + "/sleep" + str(x_arr[x]) + '/drtmh-nocc' + alg + '-' + "ycsb" + '-4-' + version + '.log_0'
            num[which].append(get_tput(fname))
    rects_list=[]

    for x in range(len(algs)):
        rects_list.append(plt.plot(real_x,num[x], ls='-', c=colors[x], lw=2, marker=markers[x], label=algs[x]))

    objs = [str(x) for x in x_arr]
    y_pos = np.arange(len(objs)) + 1
    plt.title(version_format[version], fontsize=24, loc='center')
    if i == 0:
        plt.ylabel('Throughput (M txns/s)', fontsize=20)
    plt.xlabel(r'Txn computation ($\mu s$)', fontsize=20)
    # plt.title(apps[wl]+'_'+str(server_cnt), fontsize=8, loc='right')
    ax.get_xaxis().set_tick_params(direction='in', width=1, length=0)
    plt.xticks(y_pos, objs, fontsize=16, rotation=0)
    ax.get_yaxis().set_tick_params(direction='in', width=0.5, length=2, pad=1)
    plt.yticks(fontsize=16)
    ax.yaxis.get_offset_text().set_size(2)
    ax.yaxis.set_ticks_position('left')
    plt.ylim(ymin=0,ymax=0.55)

plt.legend(ncol=1, bbox_to_anchor=(1,1.1),fontsize=20)
plt.savefig(out_path + '/' + 'eval_exe_workload' + '.pdf', bbox_inches='tight')
