if [ $1 == "" ];then
            echo "input invalid"
                    exit
                fi

qp_num=1
if [ x$2 != "x" ];then
    qp_num=$2
fi

sed -i 's/#define ONE_SIDED_READ.*/#define ONE_SIDED_READ 1/g' ../src/tx_config.h
sed -i 's/#define RDMA_CACHE.*/#define RDMA_CACHE 1/g' ../src/tx_config.h
sed -i 's/#define TX_LOG_STYLE.*/#define TX_LOG_STYLE 2/g' ../src/tx_config.h
sed -i "s/#define LARGE_CONNECTION.*/#define LARGE_CONNECTION ${qp_num}/g" ../src/tx_config.h
make -j nocc$1-onesided


#sed 's/#define ONE_SIDED_READ/#define ONE_SIDED_READ 2/g' tx_config.h > aaa
#sed 's/#define RDMA_CACHE_/#define RDMA_CACHE 1/g' aaa > ../src/tx_config.h
#make -j12 nocc$1-hybrid


sed -i 's/#define ONE_SIDED_READ.*/#define ONE_SIDED_READ 0/g' ../src/tx_config.h
sed -i 's/#define RDMA_CACHE.*/#define RDMA_CACHE 0/g' ../src/tx_config.h
sed -i 's/#define TX_LOG_STYLE.*/#define TX_LOG_STYLE 1/g' ../src/tx_config.h
sed -i "s/#define LARGE_CONNECTION.*/#define LARGE_CONNECTION ${qp_num}/g" ../src/tx_config.h
make -j nocc$1-rpc
