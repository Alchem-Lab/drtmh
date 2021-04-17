import matplotlib
matplotlib.use("pdf")
matplotlib.rc('pdf', fonttype=42)
import sys
import subprocess
import os
import numpy as np
import matplotlib.pyplot as plt

# data to plot
apps = ('bank', 'ycsb', 'tpcc')
applabel = {'bank':'SmallBank', 'ycsb':'YCSB', 'tpcc':'TPC-C'}
#algs = ('nowait', 'waitdie', 'occ', 'mvcc', 'sundial')
algs = ('nowait', 'waitdie', 'occ', 'mvcc', 'sundial')
series = ('Read', 'Lock', 'Validate', 'Log', 'Release', 'Commit', 'Renew')
versions = ('rpc', 'onesided')
colors=['b','g','r','c','m','y','violet']
patterns = [ "","/", "/" , "\\" , "|" , "-" , "x", "o", "O", ".", "*" ]

input_dir = sys.argv[1]

def get_res(filedir):
    data = {}

    tres = 0.0
    res = ""
    if os.path.isfile(filedir):
        res = subprocess.check_output(('awk', '/read_lat time:/{print substr($4, 1, length($4)-2)}', filedir))
    else:
        print("no file: " + filedir)
    if res.strip() != "":
        tres = float(res)*1000   # change to nanoseconds
        data['Read'] = tres
    else:
        print("not num: " + filedir)

    tres = 0.0
    res = ""
    if os.path.isfile(filedir):
        res = subprocess.check_output(('awk', '/lock time:/{print substr($4, 1, length($4)-2)}', filedir))
    else:
        print("no file: " + filedir)
    if res.strip() != "":
        tres = float(res)*1000
        data['Lock'] = tres
    else:
        print("not num: " + filedir)
    
    tres = 0.0
    res = ""
    if os.path.isfile(filedir):
        res = subprocess.check_output(('awk', '/validate time:/{print substr($4, 1, length($4)-2)}', filedir))
    else:
        print("no file: " + filedir)
    if res.strip() != "":
        tres = float(res)*1000
        data['Validate'] = tres
    else:
        print("not num: " + filedir)
    
    tres = 0.0
    res = ""
    if os.path.isfile(filedir):
        res = subprocess.check_output(('awk', '/log time:/{print substr($4, 1, length($4)-2)}', filedir))
    else:
        print("no file: " + filedir)
    if res.strip() != "":
        tres = float(res)*1000
        data['Log'] = tres
    else:
        print("not num: " + filedir)

    tres = 0.0
    res = ""
    if os.path.isfile(filedir):
        res = subprocess.check_output(('awk', '/release_write time:/{print substr($4, 1, length($4)-2)}', filedir))
    else:
        print("no file: " + filedir)
    if res.strip() != "":
        tres = float(res)*1000
        data['Release'] = tres
    else:
        print("not num: " + filedir)

    tres = 0.0
    res = ""
    if os.path.isfile(filedir):
        res = subprocess.check_output(('awk', '/2pc time:/{print substr($4, 1, length($4)-2)}', filedir))
    else:
        print("no file: " + filedir)
    if res.strip() != "":
        tres = float(res)*1000
        data['2PC'] = tres
    else:
        print("not num: " + filedir)
    
    tres = 0.0
    res = ""
    if os.path.isfile(filedir):
        res = subprocess.check_output(('awk', '/commit time:/{print substr($4, 1, length($4)-2)}', filedir))
    else:
        print("no file: " + filedir)
    if res.strip() != "":
        tres = float(res)*1000
        data['Commit'] = tres
    else:
        print("not num: " + filedir)

    tres = 0.0
    res = ""
    if os.path.isfile(filedir):
        res = subprocess.check_output(('awk', '/renew_lease time:/{print substr($4, 1, length($4)-2)}', filedir))
    else:
        print("no file: " + filedir)
    if res.strip() != "":
        tres = float(res)*1000
        data['Renew'] = tres
    else:
        print("not num: " + filedir)

    return data

plt.figure(figsize=(24,8))
for k, appname in enumerate(apps):
    for j, version in enumerate(versions):
        ax = plt.subplot(len(apps),len(versions), k*len(versions)+j+1)
        index = np.arange(len(algs))
        bar_width = 0.12
        opacity = 0.8

        all_data = {}
        for alg in algs:
            fname = input_dir + '/drtmh-nocc' + alg + '-' + appname + '-4-' + version + '.log_0'
            data = get_res(fname)
            for key in data:
                if not key in all_data:
                    all_data[key] = []
                all_data[key].append(data[key])
            
        for i in range(len(series)):
            rects1 = plt.bar(index + i*bar_width, all_data[series[i]], bar_width,
            alpha=opacity, color=colors[i], hatch=patterns[i], label=series[i])

        # if k == len(apps)//2 and j == 0:
        #    plt.ylabel(r'Latency($\mu$s)', fontsize=36) 
        if j == 0:
            plt.ylabel(applabel[appname], fontsize=32)
        plt.yticks(fontsize=32)
        plt.ylim(ymin=0,ymax=10)
        if k == len(apps)-1:
            plt.xticks(index + bar_width*(len(series)-1)/2, algs, fontsize=32,rotation=45)
        else:
            plt.xticks([],[])
        plt.grid(axis="y")
        if appname == "bank":
            plt.ylim(ymin=0,ymax=7)
        if appname == "ycsb":
            plt.ylim(ymin=0,ymax=40)
        if appname == "tpcc":
            plt.ylim(ymin=0,ymax=10)
        if k == 0 and j == 1:
           plt.legend(fontsize=26,ncol=7,loc=1,bbox_to_anchor=(0,0,1,1.8))

plt.savefig('lat_breakdown' + '.pdf', bbox_inches='tight')
