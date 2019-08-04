#! /usr/bin/env python

import matplotlib as mpl
mpl.use('pdf')

import matplotlib.pyplot as plt
import numpy as np
import os.path
import sys
import subprocess


# if len(sys.argv) != 2:
#     print("param!")
#     exit(-1)
default_out_path = "out"
default_plot_path = "plot"

ccalgs = ['noccocc', 'noccnowait', 'noccwaitdie', 'noccmvcc', 'nocccalvin', 'noccsundial']
shortcc = {'noccocc':'occ', 'noccnowait':'nowait', 'noccwaitdie':'waitdie', 'noccmvcc':'mvcc', 'nocccalvin':'calvin', 'noccsundial':'sundial'}
versions = ['rpc','onesided','hybrid']
apps = ['bank', 'ycsb', 'tpcc']

mac_nums = [2,4]
threads = 8

def plot_using_data(out_path, plot_path):
    if os.path.isdir(out_path) == False:
        print("%s does not exist.\n" % (out_path))
        exit()

    if os.path.isdir(plot_path) == False:
        os.system("mkdir -p " + plot_path)

    plt.clf()
    plt.figure(figsize=(9,9))

    cc_outs=[]
    for wl in range(len(apps)):
        for idx, server_cnt in enumerate(mac_nums):
            j = wl*len(mac_nums)+idx
            print(j)
            ax = plt.subplot(3, 2, j+1)

            cc_outs.append([])
            for version in versions:
                cc_outs[-1].append([])
                for inpt in ccalgs:
                    val = 0.0
                    fname = out_path + '/drtmh-' + inpt + '-' + apps[wl] + '-' + str(server_cnt) + '-' + version + '.log'
                    res = ""
                    if os.path.isfile(fname) == True:
                        ### get the avg abort ratio
                        res = subprocess.check_output(('awk', 'BEGIN{cnt=0; sum=0} /System throughput/{cnt+=1; sum+=$7} END{if (cnt == 0) print 0; else print sum/cnt}', fname))
                    if(res.strip() != ""):
                        val = float(res)*100
                    cc_outs[-1][-1].append(val)

            width = 0.3
            colors=['b','g','r','c','m','y','k','grey']
            assert(len(colors) >= len(versions))
            
            objs=[]
            for inpt in ccalgs:
                    objs.append(str(shortcc[inpt]))
            y_pos = np.arange(len(objs))*1.5

            rects_list=[]
            for idx in range(len(versions)):
                rects_list.append(plt.bar(y_pos+idx*width, cc_outs[j][idx], width, color=colors[idx], alpha=0.8, linewidth=0.1))
            
            if j % len(mac_nums) == 0:
                plt.ylabel('Abort Ratio (%)', fontsize=8)
            plt.title(apps[wl]+'_'+str(server_cnt), fontsize=8, loc='right')
            #plt.xticks([], [])
            ax.get_xaxis().set_tick_params(direction='in', width=1, length=0)
            plt.xticks(y_pos+width*len(versions)/2, objs, fontsize=6, rotation=0)
            ax.get_yaxis().set_tick_params(direction='in', width=0.5, length=2, pad=1)
            plt.yticks(fontsize=6)
            ax.yaxis.get_offset_text().set_size(2)
            ax.yaxis.set_ticks_position('left')

    plt.legend((rects_list[0][0], rects_list[1][0], rects_list[2][0]), versions, fontsize=6, bbox_to_anchor=(-1.2, -0.2, 2, .06), loc=3, ncol=2, mode="expand", borderaxespad=0.)
    plt.savefig(plot_path + '/' + 'cmp_abort_rpc_onesided' + '.pdf', bbox_inches='tight')

if len(sys.argv) < 2:
    plot_using_data(default_out_path, default_plot_path)
else:
    out_path = sys.argv[1]
    plot_using_data(out_path, out_path.replace(default_out_path, default_plot_path))
