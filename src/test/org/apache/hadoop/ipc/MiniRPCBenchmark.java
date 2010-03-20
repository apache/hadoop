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
package org.apache.hadoop.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

/**
 * MiniRPCBenchmark measures time to establish an RPC connection 
 * to a RPC server.
 * It sequentially establishes connections the specified number of times, 
 * and calculates the average time taken to connect.
 * 
 * Input arguments:
 * <ul>
 * <li>numIterations - number of connections to establish</li>
 * <li>logLevel - logging level, see {@link Level}</li>
 * </ul>
 */
public class MiniRPCBenchmark {
  private Level logLevel;

  MiniRPCBenchmark(Level l) {
    logLevel = l;
  }

  public static interface MiniProtocol extends VersionedProtocol {
    public static final long versionID = 1L;
  }

  /**
   * Primitive RPC server, which
   * allows clients to connect to it.
   */
  static class MiniServer implements MiniProtocol {
    private static final String DEFAULT_SERVER_ADDRESS = "0.0.0.0";

    private Server rpcServer;

    @Override // VersionedProtocol
    public long getProtocolVersion(String protocol, 
                                   long clientVersion) throws IOException {
      if (protocol.equals(MiniProtocol.class.getName()))
        return versionID;
      throw new IOException("Unknown protocol: " + protocol);
    }

    /** Start RPC server */
    MiniServer(Configuration conf)
    throws IOException {
      rpcServer = RPC.getServer(
          this, DEFAULT_SERVER_ADDRESS, 0, 1, false, conf);
      rpcServer.start();
    }

    /** Stop RPC server */
    void stop() {
      if(rpcServer != null) rpcServer.stop();
      rpcServer = null;
    }

    /** Get RPC server address */
    InetSocketAddress getAddress() {
      if(rpcServer == null) return null;
      return NetUtils.getConnectAddress(rpcServer);
    }
  }

  long connectToServer(Configuration conf, InetSocketAddress addr)
  throws IOException {
    MiniProtocol client = null;
    try {
      long start = System.currentTimeMillis();
      client = (MiniProtocol) RPC.getProxy(MiniProtocol.class,
          MiniProtocol.versionID, addr, conf);
      long end = System.currentTimeMillis();
      return end - start;
    } finally {
      RPC.stopProxy(client);
    }
  }

  static void setLoggingLevel(Level level) {
    LogManager.getLogger(Server.class.getName()).setLevel(level);
    LogManager.getLogger(Client.class.getName()).setLevel(level);
  }

  /**
   * Run MiniBenchmark with MiniServer as the RPC server.
   * 
   * @param conf - configuration
   * @param count - connect this many times
   * @return average time to connect
   * @throws IOException
   */
  long runMiniBenchmark(Configuration conf,
                        int count) throws IOException {
    MiniServer miniServer = null;
    try {
      // start the server
      miniServer = new MiniServer(conf);
      InetSocketAddress addr = miniServer.getAddress();

      connectToServer(conf, addr);
      // connect to the server count times
      setLoggingLevel(logLevel);
      long elapsed = 0L;
      for(int idx = 0; idx < count; idx ++) {
        elapsed += connectToServer(conf, addr);
      }
      return elapsed;
    } finally {
      if(miniServer != null) miniServer.stop();
    }
  }

  static void printUsage() {
    System.err.println(
        "Usage: MiniRPCBenchmark <numIterations> [<logLevel>]");
    System.exit(-1);
  }

  public static void main(String[] args) throws Exception {
    System.out.println("Benchmark: RPC session establishment.");
    if(args.length < 1)
      printUsage();

    Configuration conf = new Configuration();
    int count = Integer.parseInt(args[0]);
    Level l = Level.ERROR;
    if(args.length > 1)
      l = Level.toLevel(args[1]);

    MiniRPCBenchmark mb = new MiniRPCBenchmark(l);
    long elapsedTime = 0;
      String auth = "simple";
      System.out.println(
          "Running MiniRPCBenchmark with " + auth + " authentication.");
      elapsedTime = mb.runMiniBenchmark(conf, count);
    System.out.println(org.apache.hadoop.util.VersionInfo.getVersion());
    System.out.println("Number  of  connects: " + count);
    System.out.println("Average connect time: " + ((double)elapsedTime/count));
  }
}
