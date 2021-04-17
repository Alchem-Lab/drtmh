# -*- coding: utf-8 -*-
import matplotlib
matplotlib.use("pdf")
matplotlib.rc('pdf', fonttype=42)

import matplotlib.pyplot as plt
import numpy as np
import sys,os
import subprocess
colors=['b','g','r','c','m','y','k','grey']
algs = ['nowait', 'waitdie', 'occ', 'mvcc', 'sundial']
algs_legend = {'nowait':'NW', 'waitdie':'WD', 'occ':'OC', 'mvcc':'MV', 'sundial':'SD'}
gorgon_path = sys.argv[1]
out_path = '.'
if len(sys.argv) == 3:
    out_path = sys.argv[2]

apps = ['bank', 'ycsb', 'tpcc']
app_format = {'bank':'SmallBank', 'ycsb':'YCSB', 'tpcc':'TPC-C'}
versions = ['tcp','rpc','onesided','hybrid']
version_format = {'rpc':'RPC', 'onesided':'one-sided','tcp':'TCP','hybrid':'hybrid'}

patterns = [ "","/", "/" , "\\" , "|" , "-" , "+" , "x", "o", "O", ".", "*" ]

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
            if l.find("bench_listener2.cc:295") != -1:
                res = float(l.split("average latency:")[1].strip("ms.\n"))
                tres = res
                break
            l = f.readline()
    return tres    

def get_network_roundtrips(filedir):
    tres = 0
    res = ""
    if os.path.isfile(filedir):
        res = subprocess.check_output(('awk', '/ 18:/{print $3}', filedir)).decode("utf-8")
    if res.strip() != "":
        tres = int(res)
    else:
        pass
    return tres

def get_res(filedir, num):
    if num == 0:
        return get_tput(filedir)
    elif num == 1:
        return get_lat(filedir)
    elif num == 2:
        return get_abort(filedir)
    elif num == 3:
        return get_network_roundtrips(filedir)

def set_title(app_idx, num, plt):
    title = ""
    if app_idx == 0 and num == 0:
        title += "Throughput (M txns/s)"
    elif app_idx == 0 and num == 1:
        title += "Latency (ms)"
    elif app_idx == 0 and num == 2:
        title += "Abort Rate"
    elif app_idx == 0 and num == 3:
        title += "Network Round Trips"
    plt.title(title, fontsize=16, loc='center')

def autolabel(ax, rects, height_pos):
        """
            Attach a text label above each bar displaying its height
        """
        for rect in rects:
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width()/2., height_pos,
                    '%.2f' % float(height),
                    ha='center', va='bottom')

# versions = ['rpc','onesided', 'hybrid']
plt.figure(figsize=(24,6))
for i, appname in enumerate(apps):
    for ptype in range(4):
        ax = plt.subplot(3,4,i*4 + ptype + 1)
        num = [[] for i in range(len(versions))]
        thedir = gorgon_path
        # if appname == "ycsb":
        #    thedir += "/ycsb_high_contention"
        for x in range(4):
            for alg in algs:
                fname = thedir + '/drtmh-nocc' + alg + '-' + appname + '-4-' + versions[x] + '.log_0'
                num[x].append(get_res(fname, ptype))

        if ptype == 0:
            # print('hybrid compared to the lower of rpc and one-sided:')
            for alg in range(len(algs)):
                # calculate speedup of hybrid version compared to lower version of RPC or one-sided.
                print('%.2f' % (num[3][alg] / min(num[1][alg],num[2][alg])))
            # print('hybrid compared to the higher of rpc and one-sided:')
            # for alg in range(len(algs)):
            #    # calculate speedup of hybrid version compared to lower version of RPC or one-sided.
            #    print('%.2f' % (num[3][alg] / max(num[1][alg],num[2][alg])))


            #if ptype == 0:
            #    fname = thedir + '/drtmh-nocc' + 'calvin' + '-' + appname + '-4-' + versions[x] + '.log_0'
            #    num[x].append(get_res(fname, ptype))

        width = 0.3
        # objs = [algs_legend[x] for x in algs]
        colors=['b','g','r','c','m','y','k','grey']
        objs = algs[:]
        #if ptype == 0:
        #    objs.append('calvin')

        rects_list = []
        y_pos = np.arange(len(objs))*1.5
        for idx in range(len(versions)):
                rects_list.append(ax.bar(y_pos+idx*width, num[idx], width, color=colors[idx], alpha=0.8, linewidth=0.1))
        
        set_title(i, ptype, plt)
        ax.get_xaxis().set_tick_params(direction='in', width=1, length=0)
        if i == len(apps)-1:
            plt.xticks(y_pos+width, objs, fontsize = 16, rotation=45)
        else:
            plt.xticks([], [])

        if ptype == 0:
            plt.ylabel(app_format[appname], fontsize=24)
        ax.get_yaxis().set_tick_params(direction='in', width=0.5, length=2, pad=1)
        plt.yticks(fontsize=12)
        ax.yaxis.get_offset_text().set_size(2)
        ax.yaxis.set_ticks_position('left')
        if ptype == 3:
            ax.ticklabel_format(axis='y', style='sci',scilimits=(0,0))
            ax.yaxis.get_offset_text().set_fontsize(16)

        #if ptype == 0:
        #    plt.ylim(ymin=1,ymax=2.5)
        if ptype == 1:
            if appname == "bank":
                plt.ylim(ymin=0.0, ymax=0.05)
                autolabel(ax, rects_list[0], 0.04)
            if appname == "ycsb":
                plt.ylim(ymin=0.0, ymax=0.3)
                autolabel(ax, rects_list[0], 0.25)
            if appname == "tpcc":
                plt.ylim(ymin=0.0, ymax=0.2)
                autolabel(ax, rects_list[0], 0.15)

# plt.title("Operation latency in Sundial running YCSB on gorgon(8 thread 10 cor)")
plt.legend((rects_list[0][0], rects_list[1][0],rects_list[2][0],rects_list[3][0]), [version_format[v] for v in versions], fontsize=16, bbox_to_anchor=(-2.2, -0.9, 1, .06), loc=3, ncol=4,  borderaxespad=0.)
plt.savefig(out_path + '/' + 'eval_overall' + '.pdf', bbox_inches='tight')
