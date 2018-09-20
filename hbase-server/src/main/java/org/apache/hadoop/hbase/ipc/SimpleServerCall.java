/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.hbase.CellScanner;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.io.ByteBufferPool;
import org.apache.hadoop.hbase.ipc.RpcServer.CallCleanup;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;

/**
 * Datastructure that holds all necessary to a method invocation and then afterward, carries the
 * result.
 */
@InterfaceAudience.Private
class SimpleServerCall extends ServerCall<SimpleServerRpcConnection> {

  final SimpleRpcServerResponder responder;
  final SimpleRpcServerRdmaResponder rdmaresponder;
  final boolean isRdma;
  //final RdmaHandler rdmahandler;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NP_NULL_ON_SOME_PATH",
      justification = "Can't figure why this complaint is happening... see below")

  SimpleServerCall(int id, final BlockingService service, final MethodDescriptor md,
      RequestHeader header, Message param, CellScanner cellScanner,
      SimpleServerRpcConnection connection, long size,
      final InetAddress remoteAddress, long receiveTime, int timeout, ByteBufferPool reservoir,
      CellBlockBuilder cellBlockBuilder, CallCleanup reqCleanup, SimpleRpcServerResponder responder) {
    super(id, service, md, header, param, cellScanner, connection, size, remoteAddress,
        receiveTime, timeout, reservoir, cellBlockBuilder, reqCleanup);
    this.responder = responder;
    this.rdmaresponder = null;
    this.isRdma = false;
  }

  SimpleServerCall(int id, final BlockingService service, final MethodDescriptor md, 
      RequestHeader header, Message param, CellScanner cellScanner, 
      SimpleServerRdmaRpcConnection rdmaconnection, long size,
      final InetAddress remoteAddress, long receiveTime, int timeout, ByteBufferPool reservoir,
      CellBlockBuilder cellBlockBuilder, CallCleanup reqCleanup, SimpleRpcServerRdmaResponder rdmaresponder) {
    super(id, service, md, header, param, cellScanner, rdmaconnection, size, remoteAddress, receiveTime, timeout,
        reservoir, cellBlockBuilder, reqCleanup);
    this.rdmaresponder = rdmaresponder;
    this.responder = null;
    this.isRdma = true;
  }

  /**
   * Call is done. Execution happened and we returned results to client. It is now safe to cleanup.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "IS2_INCONSISTENT_SYNC",
      justification = "Presume the lock on processing request held by caller is protection enough")
  @Override
  public void done() {
    super.done();
    this.getConnection().decRpcCount(); // Say that we're done with this call.
  }

  @Override///TODO rgy add rdma supplicant of this fun
  public synchronized void sendResponseIfReady() throws IOException {
    // set param null to reduce memory pressure
    //SimpleRpcServer.LOG.warn("RDMA ？ sendResponseIfReady");
    this.param = null;
    if(this.isRdma){
      SimpleRpcServer.LOG.warn("RDMA: sendResponseIfReady");
      SimpleServerRdmaRpcConnection.processResponse(this.rdmaconn,this);//TODO change to rdmahandler
    }else{
      this.responder.doRespond(getConnection(), this);
    }
  }

  SimpleServerRpcConnection getConnection() {
    return this.connection;
  }
}
