package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.hbase.util.Pair;

import java.util.HashMap;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Public
public class RdmaConnectionPool {
    private RdmaNative rn;
    
    public RdmaConnectionPool(RdmaNative rni) {
        rn = rni;
        pool = new HashMap<>();
    }
    private HashMap<Pair<String, Integer>, Pair<RdmaNative.RdmaMuxedClientConnection, Integer>> pool;
    // Acquire a connection to addr:port
    public RdmaNative.RdmaMuxedClientConnection acquire(String addr, int port) throws RdmaConnectException{
        Pair<String, Integer> pr = new Pair<>(addr, port);
        if(pool.containsKey(pr)){
            Pair<RdmaNative.RdmaMuxedClientConnection, Integer> rconn = pool.get(pr);
            rconn.setSecond(rconn.getSecond() + 1);
            return rconn.getFirst();
        }
        // we need to allocate new connection
        RdmaNative.RdmaMuxedClientConnection rmcc = rn.muxedConnect(addr, port);
        if(rmcc == null) throw new RdmaConnectException(addr, port);
        Pair <RdmaNative.RdmaMuxedClientConnection, Integer> connref = new Pair<>(rmcc, 1);
        pool.put(pr, connref);
        return rmcc;
    }
    // release the connection acquired by acquire()
    public void release(RdmaNative.RdmaMuxedClientConnection rmcc) throws IllegalArgumentException, RdmaReleaseException {
        Pair<String, Integer> pr = new Pair<>(rmcc.addr, rmcc.port);
        if(!(pool.containsKey(pr))) throw new IllegalArgumentException();
        Pair <RdmaNative.RdmaMuxedClientConnection, Integer> connref = pool.get(pr);
        if(connref.getSecond() <= 0) throw new RdmaReleaseException(rmcc.addr, rmcc.port);
        connref.setSecond(connref.getSecond() - 1);
    }
    // release the connection acquired by acquire(), and if nobody continues reference it, close the connection
    public void releaseAndClose(RdmaNative.RdmaMuxedClientConnection rmcc) throws IllegalArgumentException, RdmaReleaseException {
        Pair<String, Integer> pr = new Pair<>(rmcc.addr, rmcc.port);
        if(!(pool.containsKey(pr))) throw new IllegalArgumentException();
        Pair <RdmaNative.RdmaMuxedClientConnection, Integer> connref = pool.get(pr);
        if(connref.getSecond() <= 0) throw new RdmaReleaseException(rmcc.addr, rmcc.port);
        if(connref.getSecond() == 1) {
            rmcc.close();
            pool.remove(pr);
            return;
        }
        connref.setSecond(connref.getSecond() - 1);

    }
    // close every 0 reference connections in the pool, return how much connection closed
    public int shrink() {
        int relcount = 0;
        for(HashMap.Entry<Pair<String, Integer>, Pair<RdmaNative.RdmaMuxedClientConnection, Integer>>
                entry: pool.entrySet()){
            if(entry.getValue().getSecond() == 0){
                entry.getValue().getFirst().close();
                pool.remove(entry.getKey());
                relcount ++;
            }
        }
        return relcount;
    }
    // enforcing the close of certain connection, regardless its reference counts
    public void shutdown(String addr, int port) throws IllegalArgumentException {
        Pair<String, Integer> pr = new Pair<>(addr, port);
        if(!(pool.containsKey(pr))) throw new IllegalArgumentException();
        Pair <RdmaNative.RdmaMuxedClientConnection, Integer> connref = pool.get(pr);
        connref.getFirst().close();
        pool.remove(pr);
    }
    public void shutdown(RdmaNative.RdmaMuxedClientConnection rmcc) throws IllegalArgumentException {
        shutdown(rmcc.addr, rmcc.port);
    }
    // close every connection in the pool, and clean all entries from pool
    public void finalize(){
        for(HashMap.Entry<Pair<String, Integer>, Pair<RdmaNative.RdmaMuxedClientConnection, Integer>>
                entry: pool.entrySet()){
            entry.getValue().getFirst().close();
        }
        pool = new HashMap<>();
    }
    public class RdmaConnectException extends Exception{
        private String addr;
        private int port;
        public RdmaConnectException(String iaddr, int iport){
            addr = iaddr;
            port = iport;
        }
        @Override
        public String toString() {
            return String.format("Rdma Connect to %s:%d failed.", addr, port);
        }
    }
    public class RdmaReleaseException extends Exception {
        private String addr;
        private int port;
        public RdmaReleaseException(String iaddr, int iport){
            addr = iaddr;
            port = iport;
        }

        @Override
        public String toString() {
            return String.format("Over-release on RDMA Connection -> %s:%d", addr, port);
        }
    }

}
