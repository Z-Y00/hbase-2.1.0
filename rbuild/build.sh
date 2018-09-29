#!/bin/bash

m2=-Dmaven.repo.local=$PWD/.m2
mkdir -p ./rbuild/bin/lib

cd ./hbase-client/
mvn install package -DskipTests $m2
#important to install it locally, for it will be used in the ../hbase-server/
cp ./target/hbase-client-2.1.0.jar ../rbuild/bin/lib


cd ../hbase-server/
mvn package -DskipTests $m2
cp ./target/hbase-server-2.1.0.jar ../rbuild/bin/lib

cd ..

mvn -DskipTests package assembly:single $m2

cd ./hbase-assembly/target/
cp ./*.gz  ../rbuild/bin/
exit
