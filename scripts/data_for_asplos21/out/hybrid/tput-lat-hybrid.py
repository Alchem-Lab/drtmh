# -*- coding: utf-8 -*-
import matplotlib
matplotlib.use("pdf")
matplotlib.rc('pdf', fonttype=42)

import matplotlib.pyplot as plt
import numpy as np
import math
import sys,os
import random,subprocess

colors=['b','g','r','c','m','y','k','grey']
markers=['.', 'o', '+']

algs = ['nowait', 'waitdie', 'occ', 'mvcc', 'sundial']
apps = ['bank','ycsb','tpcc']
apptitle = {'bank': 'SmallBank', 'ycsb':'YCSB'}

input_path = sys.argv[1]
out_path = '.'
if len(sys.argv) == 3:
    out_path = sys.argv[2]

plt.figure(figsize=(19,6))

for j, appname in enumerate(apps):
    
    for i, item in enumerate(algs):
            ax = plt.subplot(len(apps),len(algs), j*len(algs) + i + 1)

            tput = []
            lat = []
            tres = 0.0
            lres = 0.0
            path = input_path + '/' + appname + '/' + item + '/'
            if not os.path.exists(path):
                continue
            all_hybrid_dirs = subprocess.check_output('ls ' + path, shell=True).decode('ascii').split('\n')
            filenames_w_path = [path + d for d in all_hybrid_dirs]

            for thename in filenames_w_path:
                    fname = thename + '/drtmh-nocc' + item + '-' + appname + '-4-' + 'hybrid' + '.log_0'
                    if os.path.isfile(fname):
                            # print(fname)
                            res = subprocess.check_output(('awk', '-f', 'tput.awk', fname))
                            # print(res)
                            tres = (float(res.decode('ascii').strip("\n")))
                            f = open(fname)
                            l = f.readline()
                            while(l):
                                    if l.find("bench_listener2.cc:295") != -1:
                                            res = float(l.split("average latency:")[1].strip("ms.\n"))
                                            lres = res
                                            break
                                    l = f.readline()
                    if(tres == 0.0 or lres == 0.0):
                            continue
                    tput.append(tres)
                    lat.append(lres)
            # print(tput)
            # print(lat)
            width = 0.3
            myco = ['b','g','r']
            plt.scatter(tput, lat, ls='-', c=myco[0])

            thename = './rpc_onesided/'
            if not os.path.exists(thename):
                continue

            tres = 0.0
            lres = 0.0
            fname = thename + '/drtmh-nocc' + item + '-' + appname + '-4-' + 'rpc' + '.log_0'
            if os.path.isfile(fname):
                    # print(fname)
                    res = subprocess.check_output(('awk', '-f', 'tput.awk', fname))
                    # print(res)
                    tres = (float(res.decode('ascii').strip("\n")))
                    f = open(fname)
                    l = f.readline()
                    while(l):
                            if l.find("bench_listener2.cc:295") != -1:
                                    res = float(l.split("average latency:")[1].strip("ms.\n"))
                                    lres = res
                                    break
                            l = f.readline()
            if(tres == 0.0 or lres == 0.0):
                    continue
            plt.scatter([tres], [lres], marker='x', c=myco[1])

            tres = 0.0
            lres = 0.0
            fname = thename + '/drtmh-nocc' + item + '-' + appname + '-4-' + 'onesided' + '.log_0'
            if os.path.isfile(fname):
                    # print(fname)
                    res = subprocess.check_output(('awk', '-f', 'tput.awk', fname))
                    # print(res)
                    tres = (float(res.decode('ascii').strip("\n")))
                    f = open(fname)
                    l = f.readline()
                    while(l):
                            if l.find("bench_listener2.cc:295") != -1:
                                    res = float(l.split("average latency:")[1].strip("ms.\n"))
                                    lres = res
                                    break
                            l = f.readline()
            if(tres == 0.0 or lres == 0.0):
                    continue
            plt.scatter([tres], [lres], marker='+', c=myco[2])

            if j == 0:
                plt.title(item, fontsize=30, loc='right')
            if i == 0:
                plt.ylabel('Latency', fontsize=24)
            if j == 2:
                plt.xlabel('Tput', fontsize=24)
            # plt.title(apps[wl]+'_'+str(server_cnt), fontsize=8, loc='right')
            ax.get_xaxis().set_tick_params(direction='in', width=1, length=2)
            # plt.xticks(y_pos+width*3/2, objs, fontsize=6, rotation=0)
            ax.get_yaxis().set_tick_params(direction='in', width=0.5, length=2, pad=1)
            plt.yticks(fontsize=16)
            if j == 0:
                plt.xticks(np.arange(math.floor(min(tput)), math.floor(max(tput))+1, 1.0),fontsize=16)
            if j == 1:
                plt.xticks(np.arange(int(min(tput)*10)/10.0, int(max(tput)*10+2)/10.0, 0.1),fontsize=16)
            if j == 2:
                plt.xticks(np.arange(int(min(tput)*10)/10.0, int(max(tput)*10+1)/10.0, 0.2),fontsize=16)

            ax.yaxis.get_offset_text().set_size(2)
            ax.yaxis.set_ticks_position('left')
           
            #if appname == "bank":
            #    ax.xaxis.set_major_locator(plt.LinearLocator(5))
            #if appname == "ycsb":
            #    ax.xaxis.set_major_locator(plt.LinearLocator(5))
            #if appname == "tpcc":
            #    ax.xaxis.set_major_locator(plt.LinearLocator(5))
    #if j == 0:
    #    plt.legend(fontsize=30, ncol=3, bbox_to_anchor=(0.3,1.7))
    # plt.suptitle("Throughput-Latency")
    # plt.title('Interesting Graph',loc ='left')
# plt.show()
    plt.savefig(out_path + '/' + 'hybrid_latency-tput.pdf', bbox_inches='tight')
