#!/bin/bash
#ID='-i /home/rgy/.ssh/aliyun/id_rsa -P 25568  rdma_match@proxy.recolic.net'
#ID=' -i ~/.ssh/aliyun/id_rsa  rdma_match@202.114.10.172'
cd ./hbase-server/
mvn package -DskipTests
cp ./target/hbase-server-2.1.0.jar ../rbuild/bin/lib
cd ../hbase-client/
mvn package -DskipTests
cp ./target/hbase-client-2.1.0.jar ../rbuild/bin/lib
#find . -name 'hbase-server-2.1.0.jar'  -exec cp '{}' ./rbuild/bin/lib \;
#find . -name 'hbase-client-2.1.0.jar'  -exec cp '{}' ./rbuild/bin/lib \;
cd ../rbuild/bin/lib
echo "build done. let's deploy it"
mv ./* /home/rdma_match/hbase-2.1.0/lib
echo done.


