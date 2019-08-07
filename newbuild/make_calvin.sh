sed -i 's/^#define ONE_SIDED_READ.*$/#define ONE_SIDED_READ 1/g' ../src/tx_config.h
sed -i 's/^#define RDMA_CACHE.*$/#define RDMA_CACHE 1/g' ../src/tx_config.h
make -j12 nocccalvin-onesided


sed -i 's/^#define ONE_SIDED_READ.*$/#define ONE_SIDED_READ 2/g' ../src/tx_config.h
sed -i 's/^#define RDMA_CACHE.*$/#define RDMA_CACHE 1/g' ../src/tx_config.h
make -j12 nocccalvin-hybrid


sed -i 's/^#define ONE_SIDED_READ.*$/#define ONE_SIDED_READ 0/g' ../src/tx_config.h
sed -i 's/^#define RDMA_CACHE.*$/#define RDMA_CACHE 0/g' ../src/tx_config.h
make -j12 nocccalvin-rpc
