#! /usr/bin/env python

## print from python3
from __future__ import print_function

import os.path
import sys
import subprocess
import xml.etree.cElementTree as ET

from subprocess import Popen, PIPE
from subprocess import check_output

## user defined packages
from run_util import print_with_tag
from run_util import bcolors
from run_util import PrintTee
from run_util import change_to_parent

out_path = 'out'
if os.path.isdir(out_path):
    os.system('rm -fr ' + out_path)
os.system('mkdir -p ' + out_path)

# ccalgs = ['noccocc']
ccalgs = ['noccocc', 'noccnowait', 'noccwaitdie']
# versions = ['ONESIDED']
versions = ['RPC','ONESIDED']
apps = ['bank']
# apps = ['bank', 'tpcc']
mac_nums = [2]
# mac_nums = [2,4,6,8]
threads = 8
coroutines = 10

def build_hosts(f, mac_set):
    tree = ET.ElementTree(file=f)
    root = tree.getroot()
    assert root.tag == "hosts"
    
    macs = root.find('macs')
    macs.clear();
    for mac in mac_set:
        m = ET.SubElement(macs, 'a');
        m.text = str(mac)
    # ET.dump(root)
    tree.write(f)
    return

def check_huge_page(mac_num):
    failed_mac_set = {}
    output = check_output(["./cat_hugepages.sh", str(16-mac_num+1), str(16)])
    for line in output.split('\n'):
        l = (line.split(' '))
        if len(l) < 3:
            continue
        mac = l[0]
        huge_page_num = int(l[2])
        if (huge_page_num < 8192):
            print("check failed: mac %s huge page %f" % (mac,huge_page_num))
        else:
            print("check success: mac %s huge page %f" % (mac,huge_page_num))

def main():
    for alg in ccalgs:
        for version in versions:
            print("building " + alg + " " + version + "...")
            os.system('sed -i \'s/^#define ONE_SIDED_READ.*$/#define ONE_SIDED_READ ' + (str(1) if version == 'ONESIDED' else str(0)) + '/g\' ../src/tx_config.h')
            code = os.system('cd ../build/ && make -j ' + alg) >> 8
            if code != 0:
                print("Compilation Failed.\n")
                continue

            for app in apps:
                for scale in mac_nums:
                    mac_set = ["nerv" + str(16-i) for i in range(scale)][::-1]
                    build_hosts("hosts.xml", mac_set)
                    check_huge_page(scale)
                    outfilename = "drtmh-" + alg + '-' + app + '-' + str(scale) + '-' + version;

                    subprocess.call(["salloc", "-N", "%d" % scale, "-t", "00:02:00", "--nodelist=%s" % ','.join(mac_set), 
                        "./run2.py", "config.xml", alg, "-t " + str(threads) + " -c " + str(coroutines) + " -r 100", 
                        app, "%d" % scale, ','.join(mac_set), out_path + '/' + outfilename + '.log'], stderr=subprocess.STDOUT)

    d = subprocess.check_output(('date', '-Iminutes'))
    os.system('mv ' + out_path + ' ' + out_path + '_' + ''.join(d.strip().split(':')))

if __name__ == "__main__":
    main()
