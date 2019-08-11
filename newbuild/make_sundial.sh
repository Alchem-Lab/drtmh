if [ $1 == "" ];then
            echo "input invalid"
                    exit
                fi
sed 's/#define ONE_SIDED_READ/#define ONE_SIDED_READ 1/g' tx_config.h > aaa
sed 's/#define RDMA_CACHE_/#define RDMA_CACHE 1/g' aaa > ../src/tx_config.h
make -j12 nocc$1-onesided


#sed 's/#define ONE_SIDED_READ/#define ONE_SIDED_READ 2/g' tx_config.h > aaa
#sed 's/#define RDMA_CACHE_/#define RDMA_CACHE 1/g' aaa > ../src/tx_config.h
#make -j12 nocc$1-hybrid


sed 's/#define ONE_SIDED_READ/#define ONE_SIDED_READ 0/g' tx_config.h > aaa
sed 's/#define RDMA_CACHE_/#define RDMA_CACHE 0/g' aaa > ../src/tx_config.h
make -j12 nocc$1-rpc
