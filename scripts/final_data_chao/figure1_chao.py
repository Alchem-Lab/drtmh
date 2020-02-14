# -*- coding: utf-8 -*-
import matplotlib
matplotlib.use("pdf")

import matplotlib.pyplot as plt
import numpy as np
import sys,os
import subprocess
versions = ['rpc','onesided']
# versions = ['rpc','onesided', 'hybrid']
colors=['b','g','r','c','m','y','k','grey']
algs = ['nowait', 'waitdie', 'occ', 'mvcc', 'sundial', 'calvin']
less_algs = ['nowait', 'waitdie', 'occ', 'mvcc', 'sundial']
algs_legend = {'nowait':'NW', 'waitdie':'WD', 'occ':'OC', 'mvcc':'MV', 'sundial':'SD', 'calvin':'CV'}
out_path = sys.argv[1]
out_path_seele = sys.argv[2]
apps = ['bank', 'ycsb', 'tpcc']
versions = ['rpc','onesided']
version_format = {'rpc':'RPC', 'onesided':'onesided'}

patterns = [ "","/", "/" , "\\" , "|" , "-" , "+" , "x", "o", "O", ".", "*" ]

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

def set_title(app_idx, num, plt):
    title = ""
    if app_idx == 0 and num < 3:
        title = "EDR "
    elif app_idx == 0:
        title = "FDR "
    if app_idx == 0 and num % 3 == 0:
        title += "Throughput (M txns/s)"
    elif app_idx == 0 and num % 3 == 1:
        title += "Latency (ms)"
    elif app_idx == 0:
        title += "Abort Rate"
    plt.title(title, fontsize=16, loc='center')

# versions = ['rpc','onesided', 'hybrid']
plt.figure(figsize=(24,6))
for i, appname in enumerate(apps):
    for ptype in range(5):
        ax = plt.subplot(3,5,i*5 + ptype + 1)
        num = [[],[]]
        thedir = out_path
        if ptype > 2:
            thedir = out_path_seele
        for x in range(2):
            for alg in algs:
                fname = thedir + '/drtmh-nocc' + alg + '-' + appname + '-4-' + versions[x] + '.log_0'
                num[x].append(get_res(fname, ptype))
        width = 0.3
        # objs = [algs_legend[x] for x in algs]
        objs = algs
        lesso = less_algs
        colors=['b','g','r','c','m','y','k','grey']
        y_pos = np.arange(len(objs))*1.5
        lessy = np.arange(len(lesso))*1.5

        rects_list = []
        for idx in range(len(versions)):
            if ptype%3 != 1:
                rects_list.append(plt.bar(y_pos+idx*width, num[idx], width, color=colors[idx], alpha=0.8, linewidth=0.1))
            else:
                new_num = []
                for kk in range(5):
                    new_num.append(num[idx][kk])
                rects_list.append(plt.bar(lessy+idx*width, new_num, width, color=colors[idx], alpha=0.8, linewidth=0.1))
        
        set_title(i, ptype, plt)
        ax.get_xaxis().set_tick_params(direction='in', width=1, length=0)
        if i == len(apps)-1:
            if ptype % 3 != 1:
                plt.xticks(y_pos+width, objs, fontsize = 16, rotation=45)
            else:
                plt.xticks(lessy+width, lesso, fontsize = 16, rotation=45)

        else:
            plt.xticks([], [])

        if ptype == 0:
            plt.ylabel(appname, fontsize=24)
        ax.get_yaxis().set_tick_params(direction='in', width=0.5, length=2, pad=1)
        plt.yticks(fontsize=12)
        ax.yaxis.get_offset_text().set_size(2)
        ax.yaxis.set_ticks_position('left')

# plt.title("Operation latency in Sundial running YCSB on gorgon(8 thread 10 cor)")
plt.legend((rects_list[0][0], rects_list[1][0]), [version_format[v] for v in versions], fontsize=16, bbox_to_anchor=(-2.2, -0.9, 1, .06), loc=3, ncol=2,  borderaxespad=0.)
plt.savefig(out_path + '/' + 'eval_overall' + '.pdf', bbox_inches='tight')
