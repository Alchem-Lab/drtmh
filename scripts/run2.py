#! /usr/bin/env python

## include packages
import commands
import subprocess # execute commands
import sys    # parse user input
import signal # bind interrupt handler
import pickle
# from runner import RoccRunner

import time #sleep

import xml.etree.cElementTree as ET ## parsing the xml file

import threading #lock

import os  # change dir

#import zmq # network communications, if necessary

## user defined packages
from run_util import print_with_tag
from run_util import change_to_parent

#====================================#

original_sigint_handler = signal.getsignal(signal.SIGINT)

## config parameters and global parameters
## default mac set
mac_set = ["nerv14", "nerv15", "nerv16"]
mac_num = len(mac_set)
PORT = 8090
#port = 9080

## benchmark constants
DIR = "/storage/kezhaohuang/git_repos/drtmh/scripts/"
BASE_CMD = "cd " + DIR + " && ./%s --bench %s --txn-flags 1  --verbose --config %s"
output_cmd = "1>/dev/null 2>&1 &" ## this will ignore all the output
output_file_path = "drtmh.log"
NO_OUTPUT_CMD_LOG = " 1>/dev/null 2>&1 &" ## this will flush the log to dev null

FNULL = open(os.devnull, 'w')

## bench config parameters
base_cmd = ""
config_file = DIR + "config.xml"

## start parese input parameter"
program = "dbtest"

exe = ""
bench = "bank"

## lock
int_lock = threading.Lock()

# ending flag
script_end = False

## helper functions
def kill_servers(e):
    #  print "ending ... kill servers..."
    sigint = 2
    kill_cmd1 = "pkill %s --signal %d" % (e,sigint)

    # real kill
    kill_cmd2 = "pkill %s" % e
    for i in xrange(mac_num):
        subprocess.call(["ssh", "-n","-f", mac_set[i], kill_cmd1])

    # r = RoccRunner()

    for i in xrange(mac_num):
        # c = 0
        # while r.check_liveness([],mac_set[i]):
        #    time.sleep(2)
        #    c += 1
        #    if c > 5:
        #        break

        subprocess.call(["ssh", "-n","-f", mac_set[i], kill_cmd1])
        time.sleep(1)

        try:
            subprocess.call(["ssh", "-n","-f", mac_set[i], kill_cmd2])
            bcmd = "ps aux | grep nocc"
            stdout, stderr = Popen(['ssh',"-o","ConnectTimeout=2",m, bcmd],
                                   stdout=PIPE).communicate()
            assert(len(stdout.split("\n")) == 3)

        except:
            pass
    return

## singal handler
def signal_int_handler(sig, frame):

    print_with_tag("ENDING", "End benchmarks")
    global script_end
    int_lock.acquire()

    if (script_end):
        int_lock.release()
        return

    script_end = True

    print_with_tag("ENDING", "send ending messages in SIGINT handler")
    print_with_tag("ENDING", "kill processes")
    signal.signal(signal.SIGINT, original_sigint_handler)
    kill_servers(exe)
    print_with_tag("ENDING", "kill processes done")
    time.sleep(1)
    int_lock.release()

    sys.exit(0)
    return

def parse_input():
    global output_file_path, config_file, exe, base_cmd, bench, mac_num, mac_set ## global declared parameters
    if (len(sys.argv)) > 1: ## config file name
        config_file = sys.argv[1]
    if (len(sys.argv)) > 2: ## exe file name
        exe = sys.argv[2]
    if (len(sys.argv)) > 3: ## program specified args
        args = sys.argv[3]
    if (len(sys.argv)) > 4:
        bench = sys.argv[4]
    if (len(sys.argv)) > 5:
        mac_num = int(sys.argv[5])
    if (len(sys.argv)) > 6:
        mac_set = sys.argv[6].split(',')
        assert(mac_num == len(mac_set))
    else:
        mac_set = ["nerv"+str(16-i) for i in range(mac_num)][::-1]
    print("in run2.py mac_set is " + str(mac_set))
    if (len(sys.argv)) > 7:
        output_file_path = sys.argv[7];
    args += (" -p %d" % mac_num)

    base_cmd = (BASE_CMD % (exe, bench, config_file)) + " --id %d " + args
    return

def start_servers():
    global base_cmd, mac_num, mac_set
    assert(len(mac_set) == mac_num)
    for i in xrange(0,mac_num):
        if i == 0:
            OUTPUT_CMD_LOG = " 1>" + output_file_path + " 2>&1 &" ## this will flush the log to a file
            cmd = (base_cmd % (i)) + OUTPUT_CMD_LOG ## disable remote output
        else:
            OUTPUT_CMD_LOG = " 1>" + output_file_path + "_" + str(i) + " 2>&1 &" ## this will flush the log to a file
            #cmd = (base_cmd % (i)) + NO_OUTPUT_CMD_LOG
            cmd = (base_cmd % (i)) + OUTPUT_CMD_LOG 
        print ' '.join(["ssh", "-n","-f", mac_set[i], "\"" + cmd + "\""])
        subprocess.call(["ssh", "-n","-f", mac_set[i], cmd], stderr=subprocess.STDOUT)
    return

#====================================#
## main function
def main():

    parse_input() ## parse input from command line
    print "[START] Input parsing done."
    print "[START] cleaning remaining processes."

    time.sleep(1) ## ensure that all related processes are cleaned

    signal.signal(signal.SIGINT, signal_int_handler) ## register signal interrupt handler
    start_servers() ## start server processes
    for i in xrange(10):
        ## forever loop
        time.sleep(10)
    signal_int_handler(0,1) ## 0,1 are dummy args
    subprocess.call(["scancel", "-u", "$USER"]);
    return

#====================================#
## the code
if __name__ == "__main__":
    main()
