if [ $1 == "" ];then
            echo "input invalid"
                    exit
                fi

hybrid_code=0
if [ x$2 != "x" ];then
    hybrid_code=$2
fi

sed -i 's/#define OR.*/#define OR 0/g' ../src/tx_config.h
sed -i 's/#define PA.*/#define PA 0/g' ../src/tx_config.h
sed -i '0,/#define USE_RDMA.*/{s/#define USE_RDMA.*/#define USE_RDMA 1/}' ../src/rocc_config.h
sed -i 's/#define ONE_SIDED_READ.*/#define ONE_SIDED_READ 2/g' ../src/tx_config.h
sed -i 's/#define RDMA_CACHE.*/#define RDMA_CACHE 1/g' ../src/tx_config.h
sed -i "s/#define HYBRID_CODE.*/#define HYBRID_CODE ${hybrid_code}/g" ../src/tx_config.h
sed -i "s/#define LARGE_CONNECTION.*/#define LARGE_CONNECTION 1/g" ../src/tx_config.h
sed -i 's/#define USE_TCP_MSG.*/#define USE_TCP_MSG 0/g' ../src/framework/config.h
make -j nocc$1-hybrid
