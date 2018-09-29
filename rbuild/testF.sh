#!/bin/sh
export LD_LIBRARY_PATH=/home/rdma_match/hbase-2.1.0/lib/native/Linux-amd64-64

cd /home/rdma_match/hbase-2.1.0
./kill-region.sh
./clean.sh
bin/start-hbase.sh
echo "init table"
echo "n_splits = 10
create 'test', 'f', {SPLITS => (1..n_splits).map{|i| \"user#{1000+i*(9999-1000)/n_splits}\"}}
" |  bin/hbase shell

echo "done "

cd /home/rdma_match/ycsb-hbase20-binding-0.15.0
cd ./lib
rm hbase-client-2.1.0.jar hbase-server-2.1.0.jar
cp /home/rdma_match/hbase-2.1.0/lib/*.jar ./

cd ..

bin/ycsb load hbase20 -P workloads/workloadf -cp /home/rdma_match/hbase-2.1.0/conf -p table=test -p columnfamily=f

bin/ycsb run hbase20 -P workloads/workloadf -cp /home/rdma_match/hbase-2.1.0/conf -p table=test -p columnfamily=f

cd /home/rdma_match/hbase-2.1.0
./kill-region.sh
./clean.sh
