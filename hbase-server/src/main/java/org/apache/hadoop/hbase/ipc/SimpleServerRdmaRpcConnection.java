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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.VersionInfoUtil;
import org.apache.hadoop.hbase.exceptions.RequestTooBigException;
import org.apache.hadoop.hbase.ipc.RpcServer.CallCleanup;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
//import org.apache.hadoop.hbase.io.ByteArrayInputStream;
import java.io.ByteArrayInputStream; 
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.DataOutputStream;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.util.Pair;

/** Reads calls from a connection and queues them for handling. */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "VO_VOLATILE_INCREMENT",
    justification = "False positive according to http://sourceforge.net/p/findbugs/bugs/1032/")
@InterfaceAudience.Private
class SimpleServerRdmaRpcConnection extends ServerRpcConnection {


  private RdmaNative rdma= new RdmaNative();
  public  RdmaNative.RdmaServerConnection rdmaconn;//the core of the rdmaconn class TODO init  these two
  private ByteBuff data;
  private ByteBuffer dataLengthBuffer;
  private ByteBuffer preambleBuffer;
  private ByteBuffer rbuf;
  private DataInputStream rdma_in;
  private final LongAdder rpcCount = new LongAdder(); // number of outstanding rpcs
  private long lastContact;
  final SimpleRpcServerRdmaResponder rdmaresponder;
  //final RdmaHandler rdmahandler;

  // If initial preamble with version and magic has been read or not.
  private boolean connectionPreambleRead = true;//we drop it in rdma

  final ConcurrentLinkedDeque<RpcResponse> responseQueue = new ConcurrentLinkedDeque<>();
  final Lock responseWriteLock = new ReentrantLock();
  long lastSentTime = -1L;

  public SimpleServerRdmaRpcConnection(SimpleRpcServer rpcServer,int port,
      long lastContact) {
    super(rpcServer);
    this.lastContact = lastContact;
    this.connectionHeaderRead=true;
    this.data = null;
    this.dataLengthBuffer = ByteBuffer.allocate(4);
    this.hostAddress = "10.10.0.112";//tmp fix
    try {
      this.addr=InetAddress.getByName(this.hostAddress);
    } catch (Exception e) {
      SimpleRpcServer.LOG.warn("RDMA init addr failed");
    }
    this.remotePort = port;
    this.rdmaresponder = rpcServer.rdmaresponder;
    SimpleRpcServer.LOG.warn("RDMA init rdmaconn L98 simpleserverRdmaconn.java");
    do this.rdmaconn = rdma.rdmaBlockedAccept();
         while (this.rdmaconn==null);  
    SimpleRpcServer.LOG.warn("RDMA init done!");// ??? null pointer?
  }

  public void setLastContact(long lastContact) {
    this.lastContact = lastContact;
  }

  public long getLastContact() {
    return lastContact;
  }

  /* Return true if the connection has no outstanding rpc */
  boolean isIdle() {
    return rpcCount.sum() == 0;
  }
// if it is readable , then just read into the rbuf
  boolean isReadable(){
    if (rdmaconn.isQueryReadable()) {
      this.rbuf=rdmaconn.readQuery();
      this.rbuf.rewind();
      //this.rdma_in=new DataInputStream(new ByteArrayInputStream(rbuf));
      //SimpleRpcServer.LOG.warn("RDMA isReadable get rbuf with length and content "+rbuf.remaining() +" "+ StandardCharsets.UTF_8.decode(rbuf).toString());
      return true;
    } else {
      return false;
    }
  }
  /* Decrement the outstanding RPC count */
  protected void decRpcCount() {
    rpcCount.decrement();
  }

  /* Increment the outstanding RPC count */
  protected void incRpcCount() {
    rpcCount.increment();
  }

  // //taken from stack overflow
  // public static int transferAsMuchAsPossible(ByteBuffer bbuf_dest, ByteBuffer bbuf_src) {
  //   int nTransfer = Math.min(bbuf_dest.remaining(), bbuf_src.remaining());
  //   if (nTransfer > 0) {
  //     bbuf_dest.put(bbuf_src.array(), bbuf_src.arrayOffset() + bbuf_src.position(), nTransfer);
  //     bbuf_src.position(bbuf_src.position() + nTransfer);
  //   }
  //   return nTransfer;
  // }
  public static int bufcopy(ByteBuffer src, ByteBuffer dst){
    int i=0;
    while (src.hasRemaining()&&dst.hasRemaining())
    {dst.put(src.get()); 
    i++;}
    return i;
  }

  private int readPreamble() throws IOException {
    if (preambleBuffer == null) {
      preambleBuffer = ByteBuffer.allocate(6);
    }
    preambleBuffer.rewind(); 
    int count = bufcopy(rbuf, preambleBuffer);
    SimpleRpcServer.LOG.warn("RDMA readAndProcess with count "+ count+" preambleBuffer "+preambleBuffer);
    if (count < 0 || preambleBuffer.remaining() > 0) {
      SimpleRpcServer.LOG.warn("RDMA readPreamble return with 1 ERR");
      return count;
    }
    preambleBuffer.flip();
    if (!processPreamble(preambleBuffer)) {
      SimpleRpcServer.LOG.warn("RDMA readPreamble return with -1 ERR");
      return -1;
    }
    preambleBuffer = null; // do not need it anymore
    connectionPreambleRead = true;
    return count;
  }

  private int read4Bytes() throws IOException {
    if (this.dataLengthBuffer.remaining() > 0) {
      return bufcopy(rbuf, this.dataLengthBuffer);
    } else {
      return 0;
    }
  }

  /**
   * Read off the wire. If there is not enough data to read, update the connection state with what
   * we have and returns.
   * @return Returns -1 if failure (and caller will close connection), else zero or more.
   * @throws IOException
   * @throws InterruptedException
   */
  public int readAndProcess() throws IOException, InterruptedException {//TODO RGY change to better responder
    SimpleRpcServer.LOG.warn("RDMA readAndProcess  L185");

    rbuf.rewind();
    dataLengthBuffer.rewind();
    // Try and read in an int. it will be length of the data to read (or -1 if a ping). We catch the
    // integer length into the 4-byte this.dataLengthBuffer.
    int count = read4Bytes();
    SimpleRpcServer.LOG.warn("RDMA readAndProcess read4Bytes with count2 "+ count);
    if (count < 0 || dataLengthBuffer.remaining() > 0) {
      return count;
    }

    // We have read a length and we have read the preamble. It is either the connection header
    // or it is a request.
    //if (data == null) { TODO RGY debugging always init the data buffer
      dataLengthBuffer.flip();
      int dataLength = dataLengthBuffer.getInt();
      SimpleRpcServer.LOG.warn("RDMA readAndProcess get int dataLength "+ dataLength);

      // Initialize this.data with a ByteBuff.
      // This call will allocate a ByteBuff to read request into and assign to this.data
      // Also when we use some buffer(s) from pool, it will create a CallCleanup instance also and
      // assign to this.callCleanup
      initByteBuffToReadInto(dataLength);

      // Increment the rpc count. This counter will be decreased when we write
      // the response. If we want the connection to be detected as idle properly, we
      // need to keep the inc / dec correct.
      incRpcCount();
    //}

      SimpleRpcServer.LOG.warn("RDMA rbuf data section with length "+ rbuf.remaining());
      byte[] arr = new byte[rbuf.remaining()];
      rbuf.get(arr);
      data.put(arr,0,dataLength);
      //SimpleRpcServer.LOG.warn("RDMA rbuf data section content" +" "+ StandardCharsets.UTF_8.decode(ByteBuffer.wrap(arr)).toString());
      process();

    return count;
  }

  // It creates the ByteBuff and CallCleanup and assign to Connection instance.
  private void initByteBuffToReadInto(int length) {
    // We create random on heap buffers are read into those when
    // 1. ByteBufferPool is not there.
    // 2. When the size of the req is very small. Using a large sized (64 KB) buffer from pool is
    // waste then. Also if all the reqs are of this size, we will be creating larger sized
    // buffers and pool them permanently. This include Scan/Get request and DDL kind of reqs like
    // RegionOpen.
    // 3. If it is an initial handshake signal or initial connection request. Any way then
    // condition 2 itself will match
    // 4. When SASL use is ON.
    if (this.rpcServer.reservoir == null || skipInitialSaslHandshake || !connectionHeaderRead ||
        useSasl || length < this.rpcServer.minSizeForReservoirUse) {
      this.data = new SingleByteBuff(ByteBuffer.allocate(length));
    } else {
      Pair<ByteBuff, CallCleanup> pair = RpcServer.allocateByteBuffToReadInto(
        this.rpcServer.reservoir, this.rpcServer.minSizeForReservoirUse, length);
      this.data = pair.getFirst();
      this.callCleanup = pair.getSecond();
    }
  }



  /**
   * Process the data buffer and clean the connection state for the next call.
   */
  private void process() throws IOException, InterruptedException {
    data.rewind();
    try {
      SimpleRpcServer.LOG.warn("RDMA processOneRpc");
        processOneRpc(data);
    } finally {
      dataLengthBuffer.clear(); // Clean for the next call
      data = null; // For the GC
      this.callCleanup = null;
    }
  }

  @Override
  public synchronized void close() {
    if(!rdmaconn.close())
    {
      SimpleRpcServer.LOG.warn("RDMA close failed L275");
    }
    //rdma.rdmaDestroyGlobal();
    data = null;
    callCleanup = null;
    
  }

  @Override
  public boolean isConnectionOpen() {
    return !(rdmaconn.isClosed());
  }

  @Override
  public SimpleServerCall createCall(int id, BlockingService service, MethodDescriptor md,
      RequestHeader header, Message param, CellScanner cellScanner, long size,
      InetAddress remoteAddress, int timeout, CallCleanup reqCleanup) {
        SimpleRpcServer.LOG.warn("RDMA createCall");
    return new SimpleServerCall(id, service, md, header, param, cellScanner, this, size,
        remoteAddress, System.currentTimeMillis(), timeout, this.rpcServer.reservoir,
        this.rpcServer.cellBlockBuilder, reqCleanup, this.rdmaresponder);
  }

  @Override
  protected void doRespond(RpcResponse resp) throws IOException {
    SimpleRpcServer.LOG.warn("RDMA doRespond");
    processResponse(this, resp);// this should be okey if we just respond it here,without a responder? TODO
  }
//this shouldn't be public , this should only be done via the rdma responder or handler. TODO RGY
  public static boolean processResponse(SimpleServerRdmaRpcConnection conn, RpcResponse resp) throws IOException {
    boolean error = true;
    BufferChain buf = resp.getResponse();
    try {
      // Send as much data as we can in the non-blocking fashion

      
      ByteBuffer sbuf = buf.concat();
      //rdma.rdmaRespond(conn.qp, sbuf);
      if(conn.rdmaconn.writeResponse(sbuf)) 
      error = true;
      SimpleRpcServer.LOG.warn("RDMA processResponse");
      error = false;
    } finally {
      if (error) {
        SimpleRpcServer.LOG.debug(conn + ": output error -- closing");
        resp.done();
        SimpleRpcServer.closeRdmaConnection(conn);
      }
    }

    if (!buf.hasRemaining()) {
      resp.done();
      return true;
    } else {
      // set the serve time when the response has to be sent later
      conn.lastSentTime = System.currentTimeMillis();
      return false; // Socket can't take more, we will have to come back.
    }
  }
}
