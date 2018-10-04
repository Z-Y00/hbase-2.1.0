#!/bin/bash
ID='-i /home/rgy/.ssh/aliyun/id_rsa -P 25568  rdma_match@proxy.recolic.net'


cd ./rbuild/bin/lib
echo "
cd ./hbase-2.1.0/lib/
put ./*.jar
"|sftp $ID
echo done.