## RCC

### Intro

**RCC** is a comprehensive framework for evaluating existing concurrency control protocols and creating and experimenting stage-wise hybrid designs for them.  **RCC** is atop of *DrTM+H*. RCC has been integrated with a variety of optimizations techniques such as co-routines, outstanding requests and doorbell batching.  RCC's main code is under `src/rtx`. 

------

### Build

RCC is build on top of DrTM+H. Therefore shares similar building procedure as DrTM+H.

**Dependencies:**

For build:
- CMake `>= version 3.0` (For compiling)
- libtool (for complie only)
- g++`>= 4.8`
- Boost `1.61.0` (will be automatically installed by the build tool chain, since we use a specific version of Boost)

For build & for run time
- Zmq and its C++ binding
- libibverbs 

------

**A sample of how to build RCC:**

- `git clone --recursive https://github.com/XXX/rcc.git`.
- `sudo apt-get install libzmq3-dev`
- `sudo apt-get install libtool-bin`
- `sudo apt-get install cmake` 
- `cmake -DUSE_RDMA=1              //run using RDMA; set it to be 0 if only use TCP for execution`

         `-DONE_SIDED_READ=1       // enable RDMA friendly data store， need to set to 2 for generating hybrid protocols.`
         
         `-DROCC_RBUF_SIZE_M=13240 // total RDMA buffer registered~(Unit of M)`
         
         `-DRDMA_STORE_SIZE=5000   // total RDMA left for data store~(Unit of M)`
         
         `-DRDMA_CACHE=0           // whether use location cache for data store`
         
         `-DTX_LOG_STYLE=2         // RTX's log style. 1 uses RPC, 2 uses RDMA`
         `-DHYBRID_CODE=1          // This hybrid code must be set when generating hybrid protocols.`

- `make nocc<A>-<B>`
where A is one of `occ`, `nowait`, `waitdie`, `mvcc`, `sundial`, `calvin` and B is one of `rpc`, `one-sided` and `hybrid`.

------

**Prepare for running:**

Two files: 
`config.xml`, `hosts.xml` must be used to configure the running of RCC.  
`config.xml` provides benchmark specific configuration, while `hosts.xml` provides the topology of the cluster.

The samples of these two files are listed in `${PATH_TO_RCC}/scripts` .

***

### **Run in a cluster:**

Using the following command on `each` machine `i` defined in the `hosts.xml`.

`cd ${PATH_TO_RCC}/scripts;   ./noccmvcc-rpc --bench tpcc --txn-flags 1  --verbose --config config.xml --id i -t 1 -c 1 -r 100 -p 16`


where `t` states for number of threads used, `c` states for number of coroutines used and `r` is left for workload. `tpcc` states for the application used, here states for running the TPC-C workload. use `bank` for both SmallBank and YCSB. The final augrment(16) is the number of machine used, according to the hosts.xml mentioned above. 

------

**We will soon make a detailed description and better configuration tools for RCC**.

***
