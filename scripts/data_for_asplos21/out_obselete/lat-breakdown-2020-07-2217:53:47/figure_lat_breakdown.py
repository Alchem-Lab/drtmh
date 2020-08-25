import matplotlib
matplotlib.use("pdf")
matplotlib.rc('pdf', fonttype=42)
import sys
import subprocess
import os
import numpy as np
import matplotlib.pyplot as plt

# data to plot
algs = ('nowait', 'waitdie', 'occ', 'mvcc', 'sundial')
appname = 'bank'
series = ('Read', 'Lock', 'Log', 'Release', 'Commit', 'Renew')
versions = ('rpc', 'onesided')
colors=['b','g','r','c','m','y','k','grey']
patterns = [ "","/", "/" , "\\" , "|" , "-" , "+" , "x", "o", "O", ".", "*" ]
input_dir=sys.argv[1];

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
for j, version in enumerate(versions):
    ax = plt.subplot(1,2,j+1)
    index = np.arange(len(algs))
    bar_width = 0.15
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

    if j == 0:
       plt.ylabel(r'Latency($\mu$s)', fontsize=36) 
    plt.yticks(fontsize=36)
    plt.ylim(ymin=0,ymax=12)
    plt.xticks(index + bar_width*(len(series)-1)/2, algs, fontsize=36,rotation=45)
    if j == 1:
       plt.legend(fontsize=26)

plt.savefig('lat_breakdown' + '.pdf', bbox_inches='tight')
