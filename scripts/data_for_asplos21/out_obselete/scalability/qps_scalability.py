# -*- coding: utf-8 -*-
import matplotlib
matplotlib.use("pdf")
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
apps = ['ycsb']
versions = ['rpc','onesided']
version_format = {'rpc':'RPC', 'onesided':'one-sided'}

markers = ['x', '1', '.', '^', 'd', 's']
patterns = [ "","/", "/" , "\\" , "|" , "-" , "+" , "x", "o", "O", ".", "*" ]
qps = [1,10,20,30,40]
# qps = [1,2,3,4,5,6,7,8,9]

def get_tput(filedir):
    tres = 0.0
    res = ""
    if os.path.isfile(filedir):
        res = subprocess.check_output(('awk', '-f', 'tput.awk', filedir))
    else:
        print("no file: " + filedir)
    if res.strip() != "":
        tres = float(res)
    else:
        print("not num: " + filedir)
    return tres

# versions = ['rpc','onesided', 'hybrid']
plt.figure(figsize=(10,3))
for j,appname in enumerate(apps):
    for i, version in enumerate(versions):
        ax = plt.subplot(1,2,j * 2 + i + 1)
        num = [[] for _ in range(len(algs))]
        x_arr = qps
        thepath = out_path
        for which, alg in enumerate(algs):
            for x in range(len(x_arr)):
                fname = thepath + '/qp_' + str(x_arr[x]) + '/drtmh-nocc' + alg + '-' + appname + '-4-' + version + '.log_0'
                num[which].append(round(get_tput(fname),3))
        rects_list=[]
        print(num)
        for x in range(len(algs)):
            rects_list.append(plt.plot(x_arr, num[x], ls='-', c=colors[x], lw=2, marker=markers[x], label=algs[x]))

        objs = [str(x*4) for x in x_arr]
        y_pos = np.arange(len(objs))*10 + 1
        #y_pos = np.arange(len(objs)) + 1

        if j == 0:
            plt.title(version_format[version], fontsize=24, loc='center')
        if i == 0:
            if j == 0:
                plt.ylabel('YCSB Tput (M txns/s)', fontsize=16)
        plt.xlabel('emulated machines', fontsize=16)
        #if i == 0:
        #    plt.xlabel('per-core QPs', fontsize=16)
        #else:
        #    plt.xlabel('per-core per-connection QPs', fontsize=16)
        # plt.title(apps[wl]+'_'+str(server_cnt), fontsize=8, loc='right')
        ax.get_xaxis().set_tick_params(direction='in', width=1, length=0)
        plt.xticks(y_pos, objs, fontsize=12, rotation=0)
        ax.get_yaxis().set_tick_params(direction='in', width=0.5, length=2, pad=1)
        plt.yticks(fontsize=12)
        ax.yaxis.get_offset_text().set_size(2)
        ax.yaxis.set_ticks_position('left')
        plt.ylim(ymin=0.2,ymax=0.5)
    plt.legend(bbox_to_anchor=(1, 1.1),ncol=1, fontsize=15)
    plt.savefig(out_path + '/' + 'qps_scalability' + '.pdf', bbox_inches='tight')
