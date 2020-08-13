if [ $1 == "" ];then
            echo "input invalid"
                    exit
                fi

qp_num=1
if [ x$2 != "x" ];then
    qp_num=$2
fi

sed -i 's/#define OR.*/#define OR 1/g' ../src/tx_config.h
sed -i 's/#define PA.*/#define PA 1/g' ../src/tx_config.h
sed -i '0,/#define USE_RDMA.*/{s/#define USE_RDMA.*/#define USE_RDMA 1/}' ../src/rocc_config.h
sed -i 's/#define ONE_SIDED_READ.*/#define ONE_SIDED_READ 1/g' ../src/tx_config.h
sed -i 's/#define RDMA_CACHE.*/#define RDMA_CACHE 1/g' ../src/tx_config.h
sed -i 's/#define TX_LOG_STYLE.*/#define TX_LOG_STYLE 2/g' ../src/tx_config.h
sed -i "s/#define LARGE_CONNECTION.*/#define LARGE_CONNECTION ${qp_num}/g" ../src/tx_config.h
sed -i 's/#define TX_TWO_PHASE_COMMIT_STYLE.*/#define TX_TWO_PHASE_COMMIT_STYLE 0/g' ../src/tx_config.h
sed -i 's/#define USE_TCP_MSG.*/#define USE_TCP_MSG 0/g' ../src/framework/config.h
make -j nocc$1-onesided
