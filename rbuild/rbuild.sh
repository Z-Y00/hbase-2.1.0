#!/bin/bash
ID='-i /home/rgy/.ssh/aliyun/id_rsa -P 25568  rdma_match@proxy.recolic.net'
#ID=' -i ~/.ssh/aliyun/id_rsa  rdma_match@202.114.10.172'
mkdir -p ./rbuild/bin/lib

cd ./hbase-client/
mvn install package -DskipTests
#important to install it locally, for it will be used in the ../hbase-server/
cp ./target/hbase-client-2.1.0.jar ../rbuild/bin/lib


cd ../hbase-server/
mvn package -DskipTests
cp ./target/hbase-server-2.1.0.jar ../rbuild/bin/lib


cd ../rbuild/bin/lib
echo "
cd ./hbase-2.1.0/lib/
put ./*.jar
"|sftp $ID
echo done.


