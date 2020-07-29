qp_num=1
if [ x$1 != "x" ];then
    qp_num=$1
fi

sed -i '0,/#define USE_RDMA.*/{s/#define USE_RDMA.*/#define USE_RDMA 1/}' ../src/rocc_config.h
sed -i 's/^#define ONE_SIDED_READ.*$/#define ONE_SIDED_READ 1/g' ../src/tx_config.h
sed -i 's/^#define RDMA_CACHE.*$/#define RDMA_CACHE 1/g' ../src/tx_config.h
sed -i 's/#define TX_LOG_STYLE.*/#define TX_LOG_STYLE 2/g' ../src/tx_config.h
sed -i "s/#define LARGE_CONNECTION.*/#define LARGE_CONNECTION ${qp_num}/g" ../src/tx_config.h
sed -i 's/#define TX_TWO_PHASE_COMMIT_STYLE.*/#define TX_TWO_PHASE_COMMIT_STYLE 2/g' ../src/tx_config.h
sed -i 's/#define USE_TCP_MSG.*/#define USE_TCP_MSG 0/g' ../src/framework/config.h
make -j12 nocccalvin-onesided


# sed -i 's/^#define ONE_SIDED_READ.*$/#define ONE_SIDED_READ 2/g' ../src/tx_config.h
# sed -i 's/^#define RDMA_CACHE.*$/#define RDMA_CACHE 1/g' ../src/tx_config.h
# make -j12 nocccalvin-hybrid

sed -i '0,/#define USE_RDMA.*/{s/#define USE_RDMA.*/#define USE_RDMA 1/}' ../src/rocc_config.h
sed -i 's/^#define ONE_SIDED_READ.*$/#define ONE_SIDED_READ 0/g' ../src/tx_config.h
sed -i 's/^#define RDMA_CACHE.*$/#define RDMA_CACHE 0/g' ../src/tx_config.h
sed -i 's/#define TX_LOG_STYLE.*/#define TX_LOG_STYLE 1/g' ../src/tx_config.h
sed -i "s/#define LARGE_CONNECTION.*/#define LARGE_CONNECTION ${qp_num}/g" ../src/tx_config.h
sed -i 's/#define TX_TWO_PHASE_COMMIT_STYLE.*/#define TX_TWO_PHASE_COMMIT_STYLE 1/g' ../src/tx_config.h
sed -i 's/#define USE_TCP_MSG.*/#define USE_TCP_MSG 0/g' ../src/framework/config.h
make -j12 nocccalvin-rpc

sed -i '0,/#define USE_RDMA.*/{s/#define USE_RDMA.*/#define USE_RDMA 0/}' ../src/rocc_config.h
sed -i 's/#define ONE_SIDED_READ.*/#define ONE_SIDED_READ 0/g' ../src/tx_config.h
sed -i 's/#define RDMA_CACHE.*/#define RDMA_CACHE 0/g' ../src/tx_config.h
sed -i 's/#define TX_LOG_STYLE.*/#define TX_LOG_STYLE 1/g' ../src/tx_config.h
sed -i "s/#define LARGE_CONNECTION.*/#define LARGE_CONNECTION ${qp_num}/g" ../src/tx_config.h
sed -i 's/#define TX_TWO_PHASE_COMMIT_STYLE.*/#define TX_TWO_PHASE_COMMIT_STYLE 1/g' ../src/tx_config.h
sed -i 's/#define USE_TCP_MSG.*/#define USE_TCP_MSG 1/g' ../src/framework/config.h
make -j nocccalvin-tcp
