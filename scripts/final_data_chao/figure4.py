# -*- coding: utf-8 -*-
import matplotlib
matplotlib.use("pdf")

import matplotlib.pyplot as plt
import numpy as np
import sys,os
import subprocess
versions = ['rpc','onesided']
# versions = ['rpc','onesided', 'hybrid']
colors=['b','g','r','c','y','m','k','grey']
algs = ['nowait', 'waitdie', 'occ', 'mvcc', 'sundial', 'calvin']
out_path = sys.argv[1]
out_path2 = sys.argv[2]
apps = ['bank', 'ycsb']
versions = ['rpc','onesided']
version_format = {'rpc':'RPC', 'onesided':'onesided'}

markers = ['x', '1', '.', '^', 'd', 's']
patterns = [ "","/", "/" , "\\" , "|" , "-" , "+" , "x", "o", "O", ".", "*" ]
datapointnum = 10

def get_tput(filedir):
    tres = 0.0
    res = ""
    if os.path.isfile(filedir):
        res = subprocess.check_output(('awk', 'BEGIN{cnt=0; sum=0} /System throughput/{cnt+=1; if($5~"K") sum+=$4/1000.0; else if($5~"M") sum+=$4; } END{if (cnt == 0) print 0; else print sum/cnt}', filedir))
    else:
        print("no file: " + filedir)
    if res.strip() != "":
        tres = float(res)
    else:
        print("not num: " + filedir)
    return tres

def get_abort(filedir):
    tres = 0.0
    res = ""
    if os.path.isfile(filedir):
        res = subprocess.check_output(('awk', 'BEGIN{cnt=0; sum=0} /System throughput/{cnt+=1; sum+=$7} END{if (cnt == 0) print 0; else print sum/cnt}', filedir))
    if res.strip() != "":
        tres = float(res)
    else:
        pass
    return tres

def get_lat(filedir):
    tres = 0.0
    res = ""
    if os.path.isfile(filedir):
        f = open(filedir)
        l = f.readline()
        while(l):
            if l.find("bench_listener2.cc:279") != -1:
                res = float(l.split("average latency:")[1].strip("ms.\n"))
                tres = res
                break
            l = f.readline()
    return tres    

def get_res(filedir, num):
    if num%3 == 0:
        return get_tput(filedir)
    elif num %3 == 1:
        return get_lat(filedir)
    else:
        return get_abort(filedir)

# versions = ['rpc','onesided', 'hybrid']
plt.figure(figsize=(10,7))
for j,appname in enumerate(apps):
    for i, version in enumerate(versions):
        ax = plt.subplot(2,2,j * 2 + i + 1)
        num = [[],[],[],[],[],[]]
        x_arr = []
        real_x = []
        for idx in range(datapointnum):
            # x_arr.append(2**idx)
            x_arr.append(2*idx + 1)
            real_x.append(idx + 1)
        x_arr[-1] = 20
        thepath = ""
        if j == 1:
            thepath = out_path2
        else:
            thepath = out_path
        for which, alg in enumerate(algs):
            for x in range(datapointnum):
                if alg == "calvin":
                    thepath = out_path2
                fname = thepath + "/cor" + str(x_arr[x]) + '/drtmh-nocc' + alg + '-' + appname + '-4-' + version + '.log_0'
                num[which].append(get_tput(fname))
        rects_list=[]

        x_arr[-1] = 19
        for x in range(len(algs)):
            rects_list.append(plt.plot(x_arr,num[x], ls='-', c=colors[x], lw=2, marker=markers[x], label=algs[x]))

        objs = [str(x) for x in x_arr]
        y_pos = np.arange(len(objs))*2 + 1

        if j == 0:
            plt.title(version_format[version], fontsize=24, loc='center')
        if i == 0:
            if j == 0:
                plt.ylabel('SmallBank Tput (M txns/s)', fontsize=16)
            if j == 1:
                plt.ylabel('YCSB Tput (M txns/s)', fontsize=16)
        if j == 1:
            plt.xlabel('# co-routines', fontsize=16)
        # plt.title(apps[wl]+'_'+str(server_cnt), fontsize=8, loc='right')
        ax.get_xaxis().set_tick_params(direction='in', width=1, length=0)
        plt.xticks(y_pos, objs, fontsize=12, rotation=0)
        ax.get_yaxis().set_tick_params(direction='in', width=0.5, length=2, pad=1)
        plt.yticks(fontsize=12)
        ax.yaxis.get_offset_text().set_size(2)
        ax.yaxis.set_ticks_position('left')
        if j == 0:
            plt.ylim(ymin=0,ymax=16.5)
        else:
            plt.ylim(ymin=0,ymax=0.5)
    plt.legend(loc='lower right',ncol=2)
    plt.savefig(out_path2 + '/' + 'eval_coroutines' + '.pdf', bbox_inches='tight')
