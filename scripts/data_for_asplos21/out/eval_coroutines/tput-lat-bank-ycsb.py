# -*- coding: utf-8 -*-
import matplotlib
matplotlib.use("pdf")
matplotlib.rc('pdf', fonttype=42)

import matplotlib.pyplot as plt
import numpy as np
import math
import sys,os
import random,subprocess
#versions = ['rpc','onesided']
colors=['b','g','r','c','m','y','k','grey']
markers=['.', 'o', '+']

algs = ['nowait', 'waitdie', 'occ', 'mvcc', 'sundial']
apps = ['bank','ycsb']
apptitle = {'bank': 'SmallBank', 'ycsb':'YCSB'}

versions = ['rpc','onesided','hybrid']
version_format = {'rpc':'RPC', 'onesided':'one-sided', 'hybrid':'hybrid'}

# versions = ['rpc','onesided', 'hybrid']
filenames = ["cor1","cor3","cor5","cor7","cor9","cor11"]
input_path = sys.argv[1]
out_path = '.'
if len(sys.argv) == 3:
    out_path = sys.argv[2]

plt.figure(figsize=(19,6))

for j, appname in enumerate(apps):
    
    filenames_w_path = []
    for i in range(len(filenames)):
        filenames_w_path.append(input_path + '/' + filenames[i])

    for i, item in enumerate(algs):
            ax = plt.subplot(len(apps),len(algs), j*len(algs) + i + 1)

            tput = [[] for i in range(len(versions))]
            lat = [[] for i in range(len(versions))]
            for x in range(len(versions)): # 3 version(rpc rdma)
                    # for y in range(5): # 5 protocol
                    tres = 0.0
                    lres = 0.0
                    for thename in filenames_w_path:
                            fname = thename + '/drtmh-nocc' + item + '-' + appname + '-4-' + versions[x] + '.log_0'
                            if os.path.isfile(fname):
                                    print(fname)
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
                            tput[x].append(tres)
                            lat[x].append(lres)
            print(tput)
            print(lat)
            width = 0.3
            objs = []
            if item == "sundial":
                    objs = ['read','lock&read','release write','renew_lease','commit']#,'overall'
            else:
                    objs = ['read','lock&read','release write','commit']#,'overall'
            y_pos = np.arange(len(objs))*1.5
            rects_list=[]
            myco = colors[1:]
            #iternum = 2
            #if item == "mvcc":
            #    iternum = 2
            for hkz in range(len(versions)):
                # x = [0,1,2,3,4,5,6]
                # y = [0.3,0.4,2,5,3,4.5,4]
                # for iterx in range(7):
                #     # x[iterx] = random.random()
                #     y[iterx] = random.random()
                # print(tput[hkz])
                # print(lat[hkz])
                # tput[hkz].sort()
                # lat[hkz].sort()
                rects_list.append(plt.plot(tput[hkz],lat[hkz], ls='-', c=myco[hkz], lw=2, marker=markers[hkz], label=version_format[versions[hkz]]))

            if j == 0:
                plt.title(item, fontsize=30, loc='right')
            if i == 0:
                plt.ylabel('Latency', fontsize=30)
            if j == 1:
                plt.xlabel('Tput', fontsize=30)
            # plt.title(apps[wl]+'_'+str(server_cnt), fontsize=8, loc='right')
            ax.get_xaxis().set_tick_params(direction='in', width=1, length=2)
            # plt.xticks(y_pos+width*3/2, objs, fontsize=6, rotation=0)
            ax.get_yaxis().set_tick_params(direction='in', width=0.5, length=2, pad=1)
            plt.yticks(fontsize=16)
            if j == 0:
                plt.xticks(np.arange(math.floor(min(min(tput[0]),min(tput[1]))), math.floor(max(max(tput[0]), max(tput[1])))+1, 2.0),fontsize=20)
            else:
                plt.xticks(np.arange(int(min(min(tput[0]),min(tput[1]))*10)/10.0, int(max(max(tput[0]), max(tput[1]))*10+1)/10.0, 0.1),fontsize=20)

            ax.yaxis.get_offset_text().set_size(2)
            ax.yaxis.set_ticks_position('left')
            
    # plt.legend((rects_list[0][0], rects_list[1][0], rects_list[2][0]), versions, fontsize=6, bbox_to_anchor=(-1.2, -0.2, 2, .06), loc=3, ncol=2, mode="expand", borderaxespad=0.)
    if j == 0:
        plt.legend(fontsize=30, ncol=3, bbox_to_anchor=(0.3,1.7))
    # plt.suptitle("Throughput-Latency")
    # plt.title('Interesting Graph',loc ='left')
# plt.show()
    plt.savefig(out_path + '/' + 'latency-tput.pdf', bbox_inches='tight')
