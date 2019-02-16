#! /usr/bin/env python

import subprocess
import xml.etree.cElementTree as ET

mac_set = []
host_file = "hosts.xml"

def parse_hosts(f):
    global mac_set
    tree = ET.ElementTree(file=f)
    root = tree.getroot()
    assert root.tag == "hosts"

    mac_set = []
    black_list = {}

    # parse black list
    for e in root.find("black").findall("a"):
       black_list[e.text.strip()] = True
    # parse hosts
    for e in root.find("macs").findall("a"):
        server = e.text.strip()
        if not black_list.has_key(server):
            mac_set.append(server)
    return

def main():
    parse_hosts(host_file)
  
    subprocess.call(["salloc", "-N", "%d" % len(mac_set), "-t", "00:05:00", "--nodelist=%s" % ','.join(mac_set), "./run2.py", "config.xml", "noccocc", "-t 8 -c 10 -r 100", "bank", "%d" % len(mac_set)], stderr=subprocess.STDOUT)

    
if __name__ == "__main__":
    main()
