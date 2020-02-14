# -*- coding: utf-8 -*-
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
import numpy as np
import sys,os
import subprocess
versions = ['rpc','onesided']
# versions = ['rpc','onesided', 'hybrid']
colors=['b','g','r','c','y','m','k','grey']
algs = ['occ', 'nowait', 'waitdie', 'mvcc', 'sundial', 'calvin']
out_path = sys.argv[1]
apps = ['bank', 'ycsb', 'tpcc']
versions = ['rpc','onesided']
patterns = [ "","/", "/" , "\\" , "|" , "-" , "+" , "x", "o", "O", ".", "*" ]
datapointnum = 9

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
        rects_list.append(plt.plot(real_x,num[x], ls='-', c=colors[x], lw=2, marker='.', label=algs[x]))

    objs = [str(x) for x in x_arr]
    y_pos = np.arange(len(objs)) + 1
    plt.ylabel('Throughput (M txns/s)', fontsize=16)
    plt.xlabel(r'Txn computation ($\mu s$)', fontsize=16)
    # plt.title(apps[wl]+'_'+str(server_cnt), fontsize=8, loc='right')
    ax.get_xaxis().set_tick_params(direction='in', width=1, length=0)
    plt.xticks(y_pos, objs, fontsize=12, rotation=0)
    ax.get_yaxis().set_tick_params(direction='in', width=0.5, length=2, pad=1)
    plt.yticks(fontsize=12)
    ax.yaxis.get_offset_text().set_size(2)
    ax.yaxis.set_ticks_position('left')


plt.legend(loc='lower left',ncol=2)
plt.savefig(out_path + '/' + 'eval_exe_workload' + '.pdf', bbox_inches='tight')
