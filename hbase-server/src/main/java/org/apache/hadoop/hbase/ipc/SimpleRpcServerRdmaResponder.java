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

import java.util.concurrent.ConcurrentLinkedDeque;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.util.StringUtils;


/**
 * Sends responses of RPC back to clients.
 */
@InterfaceAudience.Private
class SimpleRpcServerRdmaResponder extends Thread {

  private final SimpleRpcServer simpleRpcServer;
  private   ConcurrentLinkedDeque<SimpleServerRdmaRpcConnection> connsQueue;
  private RdmaNative rdma= new  RdmaNative();


  SimpleRpcServerRdmaResponder(SimpleRpcServer simpleRpcServer) throws IOException {
    this.simpleRpcServer = simpleRpcServer;
    this.setName("RpcServer.responder");
    this.setDaemon(true);
    this.setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER);
  }

  @Override
  public void run() {
    SimpleRpcServer.LOG.debug(getName() + ": starting");
    try {
      doRunLoop();
    } finally {
      SimpleRpcServer.LOG.info(getName() + ": stopping");
    }
  }

  private void doRunLoop() {
    long lastPurgeTime = 0; // last check for old calls.
    while (this.simpleRpcServer.running) {
      try {
        processAllResponses(connsQueue.poll());
      } catch (IOException ioe) {
        SimpleRpcServer.LOG.error(getName() + "??", ioe);
      }
    }
    SimpleRpcServer.LOG.info(getName() + ": stopped");
  }

  /**
   * Process the response for this call. You need to have the lock on
   * {@link org.apache.hadoop.hbase.ipc.SimpleServerRdmaRpcConnection#responseWriteLock}
   * @return true if we proceed the call fully, false otherwise.
   * @throws IOException
   */
  private boolean processResponse(SimpleServerRdmaRpcConnection conn, RpcResponse resp)
      throws IOException {
    boolean error = true;
    BufferChain buf = resp.getResponse();
    try {
      // Send as much data as we can in the non-blocking fashion

      SimpleRpcServer.LOG.info(" RDMA recolic: rdmaHandler::doRespond");
      ByteBuffer sbuf = buf.concat();
      if (conn.rdmaconn.writeResponse(sbuf)) // TODO
        error = true;
      SimpleRpcServer.LOG.warn("RDMA processResponse");
      error = false;

    } finally {
      if (error) {
        SimpleRpcServer.LOG.debug(conn + ": output error -- closing");
        // We will be closing this connection itself. Mark this call as done so that all the
        // buffer(s) it got from pool can get released
        resp.done();
        this.simpleRpcServer.closeRdmaConnection(conn);
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

  /**
   * Process all the responses for this connection
   * @return true if all the calls were processed or that someone else is doing it. false if there *
   *         is still some work to do. In this case, we expect the caller to delay us.
   * @throws IOException
   */
  private boolean processAllResponses(final SimpleServerRdmaRpcConnection connection)
      throws IOException {
        if (connection==null) {
          return false;//Empty connsQueue
        }
    // We want only one writer on the channel for a connection at a time.
    connection.responseWriteLock.lock();
    try {
      for (int i = 0; i < 20; i++) {
        // protection if some handlers manage to need all the responder
        RpcResponse resp = connection.responseQueue.pollFirst();
        if (resp == null) {
          return true;
        }
        if (!processResponse(connection, resp)) {
          connection.responseQueue.addFirst(resp);
          return false;
        }
      }
    } finally {
      connection.responseWriteLock.unlock();
    }

    return connection.responseQueue.isEmpty();
  }

  //
  // Enqueue a response from the application.
  //
  void doRespond(SimpleServerRdmaRpcConnection conn, RpcResponse resp) throws IOException {
    SimpleRpcServer.LOG.info("recolic: rdma responder::doRespond");
    boolean added = false;
    this.connsQueue.add(conn);//?? TODO
    // If there is already a write in progress, we don't wait. This allows to free the handlers
    // immediately for other tasks.
    if (conn.responseQueue.isEmpty() && conn.responseWriteLock.tryLock()) {
      try {
        if (conn.responseQueue.isEmpty()) {
          // If we're alone, we can try to do a direct call to the socket. It's
          // an optimization to save on context switches and data transfer between cores..
          if (processResponse(conn, resp)) {
            return; // we're done.
          }
          // Too big to fit, putting ahead.
          conn.responseQueue.addFirst(resp);
          added = true; // We will register to the selector later, outside of the lock.
        }
      } finally {
        conn.responseWriteLock.unlock();
      }
    }
    if (!added) {
      conn.responseQueue.addLast(resp);
    }
  }
}
