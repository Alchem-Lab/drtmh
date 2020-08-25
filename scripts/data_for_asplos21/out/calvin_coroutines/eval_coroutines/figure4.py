# -*- coding: utf-8 -*-
import matplotlib
matplotlib.use("pdf")
matplotlib.rc('pdf', fonttype=42)

import matplotlib.pyplot as plt
import numpy as np
import sys,os
import subprocess
versions = ['tcp','rpc','onesided']
# versions = ['rpc','onesided', 'hybrid']
colors=['b','g','r','c','y','m','k','grey']
alg = 'calvin'

input_path = sys.argv[1]
out_path = '.'
if len(sys.argv) == 3:
    out_path = sys.argv[2]

apps = ['bank', 'ycsb']
versions = ['tcp','rpc','onesided']
version_format = {'tcp':'TCP', 'rpc':'RPC', 'onesided':'one-sided'}

markers = ['x', '1', '.', '^', 'd', 's']
patterns = [ "","/", "/" , "\\" , "|" , "-" , "+" , "x", "o", "O", ".", "*" ]
datapointnum = 6

def get_tput(filedir):
    tres = 0.0
    res = ""
    if os.path.isfile(filedir):
        res = subprocess.check_output(('awk', '-f', 'tput.awk', filedir))
    else:
        print("no file: " + filedir)
    if res.strip() != "":
        tres = float(res)*1000
    else:
        print("not num: " + filedir)
    return tres

# versions = ['rpc','onesided', 'hybrid']
plt.figure(figsize=(4.2,3))
for j,appname in enumerate(apps):
        ax = plt.subplot(len(apps),1,j+1)
        
        x_arr = []
        real_x = []
        for idx in range(datapointnum):
            # x_arr.append(2**idx)
            x_arr.append(2*idx + 1)
            real_x.append(idx + 1)
        
        num = [[] for i in range(len(versions))]
        thepath = input_path
        for which, version in enumerate(versions):
            for x in range(datapointnum):
                fname = thepath + "/cor" + str(x_arr[x]) + '/drtmh-nocc' + alg + '-' + appname + '-4-' + version + '.log_0'
                num[which].append(get_tput(fname))
        rects_list=[]

        for x in range(len(versions)):
            rects_list.append(plt.plot(x_arr,num[x], ls='-', c=colors[x], lw=2, marker=markers[x], label=version_format[versions[x]]))

        objs = [str(x) for x in x_arr]
        y_pos = np.arange(len(objs))*2 + 1

        #if j == 0:
        #    plt.title(version_format[version], fontsize=24, loc='center')
        if j == 0:
            plt.ylabel('SmallBank', fontsize=16)
        if j == 1:
            plt.ylabel('YCSB', fontsize=16)
        if j == 1:
            plt.xlabel('# co-routines', fontsize=16)
        # plt.title(apps[wl]+'_'+str(server_cnt), fontsize=8, loc='right')
        ax.get_xaxis().set_tick_params(direction='in', width=1, length=0)
        if j == 0:
            plt.xticks([],[])
        else:
            plt.xticks(y_pos, objs, fontsize=16, rotation=0)
        
        ax.get_yaxis().set_tick_params(direction='in', width=0.5, length=2, pad=1)
        plt.yticks(fontsize=20)
        ax.yaxis.get_offset_text().set_size(2)
        ax.yaxis.set_ticks_position('left')
        if j == 0:
            plt.ylim(ymin=0,ymax=200)
        else:
            plt.ylim(ymin=0,ymax=20)
        if j == 1:
            plt.legend(ncol=3, bbox_to_anchor=(0, 0, 1.1, 3), fontsize=14)

plt.savefig(out_path + '/' + 'eval_calvin_coroutines' + '.pdf', bbox_inches='tight')
