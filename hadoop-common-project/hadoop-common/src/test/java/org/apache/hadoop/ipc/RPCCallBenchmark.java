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

import com.google.common.base.Joiner;
import com.google.protobuf.BlockingService;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto;
import org.apache.hadoop.ipc.protobuf.TestProtos.EchoResponseProto;
import org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos.TestProtobufRpcProto;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.MultithreadedTestUtil;
import org.apache.hadoop.test.MultithreadedTestUtil.TestContext;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Benchmark for protobuf RPC.
 * Run with --help option for usage.
 */
public class RPCCallBenchmark extends TestRpcBase implements Tool {
  private Configuration conf;
  private AtomicLong callCount = new AtomicLong(0);
  private static ThreadMXBean threadBean =
    ManagementFactory.getThreadMXBean();
  
  private static class MyOptions {
    private boolean failed = false;
    private int serverThreads = 0;
    private int serverReaderThreads = 1;
    private int clientThreads = 0;
    private String host = "0.0.0.0";
    private int port = 0;
    public int secondsToRun = 15;
    private int msgSize = 1024;
    public Class<? extends RpcEngine> rpcEngine =
        ProtobufRpcEngine.class;
    
    private MyOptions(String args[]) {
      try {
        Options opts = buildOptions();
        CommandLineParser parser = new GnuParser();
        CommandLine line = parser.parse(opts, args, true);
        processOptions(line, opts);
        validateOptions();
      } catch (ParseException e) {
        System.err.println(e.getMessage());
        System.err.println("Try \"--help\" option for details.");
        failed = true;
      }
    }

    private void validateOptions() throws ParseException {
      if (serverThreads <= 0 && clientThreads <= 0) {
        throw new ParseException("Must specify at least -c or -s");
      }
    }

    @SuppressWarnings("static-access")
    private Options buildOptions() {
      Options opts = new Options();
      opts.addOption(
        OptionBuilder.withLongOpt("serverThreads").hasArg(true)
        .withArgName("numthreads")
        .withDescription("number of server threads (handlers) to run (or 0 to not run server)")
        .create("s"));
      opts.addOption(
        OptionBuilder.withLongOpt("serverReaderThreads").hasArg(true)
        .withArgName("threads")
        .withDescription("number of server reader threads to run")
        .create("r"));

      
      opts.addOption(
        OptionBuilder.withLongOpt("clientThreads").hasArg(true)
        .withArgName("numthreads")
        .withDescription("number of client threads to run (or 0 to not run client)")
        .create("c"));

      opts.addOption(
        OptionBuilder.withLongOpt("messageSize").hasArg(true)
        .withArgName("bytes")
        .withDescription("size of call parameter in bytes")
        .create("m"));

      opts.addOption(
          OptionBuilder.withLongOpt("time").hasArg(true)
          .withArgName("seconds")
          .withDescription("number of seconds to run clients for")
          .create("t"));
      opts.addOption(
          OptionBuilder.withLongOpt("port").hasArg(true)
          .withArgName("port")
          .withDescription("port to listen or connect on")
          .create("p"));
      opts.addOption(
          OptionBuilder.withLongOpt("host").hasArg(true)
          .withArgName("addr")
          .withDescription("host to listen or connect on")
          .create('h'));
      
      opts.addOption(
          OptionBuilder.withLongOpt("engine").hasArg(true)
          .withArgName("protobuf")
          .withDescription("engine to use")
          .create('e'));
      
      opts.addOption(
          OptionBuilder.withLongOpt("help").hasArg(false)
          .withDescription("show this screen")
          .create('?'));

      return opts;
    }
    
    private void processOptions(CommandLine line, Options opts)
      throws ParseException {
      if (line.hasOption("help") || line.hasOption('?')) {
        HelpFormatter formatter = new HelpFormatter();
        System.out.println("Protobuf IPC benchmark.");
        System.out.println();
        formatter.printHelp(100,
            "java ... PBRPCBenchmark [options]",
            "\nSupported options:", opts, "");
        return;
      }

      if (line.hasOption('s')) {
        serverThreads = Integer.parseInt(line.getOptionValue('s'));
      }
      if (line.hasOption('r')) {
        serverReaderThreads = Integer.parseInt(line.getOptionValue('r'));
      }
      if (line.hasOption('c')) {
        clientThreads = Integer.parseInt(line.getOptionValue('c'));
      }
      if (line.hasOption('t')) {
        secondsToRun = Integer.parseInt(line.getOptionValue('t'));
      }
      if (line.hasOption('m')) {
        msgSize = Integer.parseInt(line.getOptionValue('m'));
      }
      if (line.hasOption('p')) {
        port = Integer.parseInt(line.getOptionValue('p'));
      }
      if (line.hasOption('h')) {
        host = line.getOptionValue('h');
      }
      if (line.hasOption('e')) {
        String eng = line.getOptionValue('e');
        if ("protobuf".equals(eng)) {
          rpcEngine = ProtobufRpcEngine.class;
        } else {
          throw new ParseException("invalid engine: " + eng);
        }
      }
      
      String[] remainingArgs = line.getArgs();
      if (remainingArgs.length != 0) {
        throw new ParseException("Extra arguments: " +
            Joiner.on(" ").join(remainingArgs));
      }
    }
    
    public int getPort() {
      if (port == 0) {
        port = NetUtils.getFreeSocketPort();
        if (port == 0) {
          throw new RuntimeException("Could not find a free port");
        }
      }
      return port;
    }

    @Override
    public String toString() {
      return "rpcEngine=" + rpcEngine + "\nserverThreads=" + serverThreads
          + "\nserverReaderThreads=" + serverReaderThreads + "\nclientThreads="
          + clientThreads + "\nhost=" + host + "\nport=" + getPort()
          + "\nsecondsToRun=" + secondsToRun + "\nmsgSize=" + msgSize;
    }
  }


  
  private Server startServer(MyOptions opts) throws IOException {
    if (opts.serverThreads <= 0) {
      return null;
    }
    conf.setInt(CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY,
        opts.serverReaderThreads);
    
    RPC.Server server;
    // Get RPC server for server side implementation
    if (opts.rpcEngine == ProtobufRpcEngine.class) {
      // Create server side implementation
      PBServerImpl serverImpl = new PBServerImpl();
      BlockingService service = TestProtobufRpcProto
          .newReflectiveBlockingService(serverImpl);

      server = new RPC.Builder(conf).setProtocol(TestRpcService.class)
          .setInstance(service).setBindAddress(opts.host).setPort(opts.getPort())
          .setNumHandlers(opts.serverThreads).setVerbose(false).build();
    } else {
      throw new RuntimeException("Bad engine: " + opts.rpcEngine);
    }
    server.start();
    return server;
  }
  
  private long getTotalCpuTime(Iterable<? extends Thread> threads) {
    long total = 0;
    for (Thread t : threads) {
      long tid = t.getId();
      total += threadBean.getThreadCpuTime(tid);
    }
    return total;
  }
  
  @Override
  public int run(String[] args) throws Exception {
    MyOptions opts = new MyOptions(args);
    if (opts.failed) {
      return -1;
    }
    
    // Set RPC engine to the configured RPC engine
    RPC.setProtocolEngine(conf, TestRpcService.class, opts.rpcEngine);

    Server server = startServer(opts);
    try {
      
      TestContext ctx = setupClientTestContext(opts);
      if (ctx != null) {
        long totalCalls = 0;
        ctx.startThreads();
        long veryStart = System.nanoTime();

        // Loop printing results every second until the specified
        // time has elapsed
        for (int i = 0; i < opts.secondsToRun ; i++) {
          long st = System.nanoTime();
          ctx.waitFor(1000);
          long et = System.nanoTime();
          long ct = callCount.getAndSet(0);
          totalCalls += ct;
          double callsPerSec = (ct * 1000000000)/(et - st);
          System.out.println("Calls per second: " + callsPerSec);
        }
        
        // Print results

        if (totalCalls > 0) {
          long veryEnd = System.nanoTime();
          double callsPerSec =
            (totalCalls * 1000000000)/(veryEnd - veryStart);
          long cpuNanosClient = getTotalCpuTime(ctx.getTestThreads());
          long cpuNanosServer = -1;
          if (server != null) {
            cpuNanosServer = getTotalCpuTime(server.getHandlers());; 
          }
          System.out.println("====== Results ======");
          System.out.println("Options:\n" + opts);
          System.out.println("Total calls per second: " + callsPerSec);
          System.out.println("CPU time per call on client: " +
              (cpuNanosClient / totalCalls) + " ns");
          if (server != null) {
            System.out.println("CPU time per call on server: " +
                (cpuNanosServer / totalCalls) + " ns");
          }
        } else {
          System.out.println("No calls!");
        }

        ctx.stop();
      } else {
        while (true) {
          Thread.sleep(10000);
        }
      }
    } finally {
      if (server != null) {
        server.stop();
      }
    }
    
    return 0;
  }


  private TestContext setupClientTestContext(final MyOptions opts)
      throws IOException, InterruptedException {
    if (opts.clientThreads <= 0) {
      return null;
    }

    // Set up a separate proxy for each client thread,
    // rather than making them share TCP pipes.
    int numProxies = opts.clientThreads;
    final RpcServiceWrapper proxies[] = new RpcServiceWrapper[numProxies];
    for (int i = 0; i < numProxies; i++) {
      proxies[i] =
        UserGroupInformation.createUserForTesting("proxy-" + i,new String[]{})
        .doAs(new PrivilegedExceptionAction<RpcServiceWrapper>() {
          @Override
          public RpcServiceWrapper run() throws Exception {
            return createRpcClient(opts);
          }
        });
    }

    // Create an echo message of the desired length
    final StringBuilder msgBuilder = new StringBuilder(opts.msgSize);
    for (int c = 0; c < opts.msgSize; c++) {
      msgBuilder.append('x');
    }
    final String echoMessage = msgBuilder.toString();

    // Create the clients in a test context
    TestContext ctx = new TestContext();
    for (int i = 0; i < opts.clientThreads; i++) {
      final RpcServiceWrapper proxy = proxies[i % numProxies];
      
      ctx.addThread(new MultithreadedTestUtil.RepeatingTestThread(ctx) {
        @Override
        public void doAnAction() throws Exception {
          proxy.doEcho(echoMessage);
          callCount.incrementAndGet();
        }
      });
    }
    return ctx;
  }

  /**
   * Simple interface that can be implemented either by the
   * protobuf or writable implementations.
   */
  private interface RpcServiceWrapper {
    public String doEcho(String msg) throws Exception;
  }

  /**
   * Create a client proxy for the specified engine.
   */
  private RpcServiceWrapper createRpcClient(MyOptions opts) throws IOException {
    InetSocketAddress addr = NetUtils.createSocketAddr(opts.host, opts.getPort());
    
    if (opts.rpcEngine == ProtobufRpcEngine.class) {
      final TestRpcService proxy = RPC.getProxy(TestRpcService.class, 0, addr, conf);
      return new RpcServiceWrapper() {
        @Override
        public String doEcho(String msg) throws Exception {
          EchoRequestProto req = EchoRequestProto.newBuilder()
            .setMessage(msg)
            .build();
          EchoResponseProto responseProto = proxy.echo(null, req);
          return responseProto.getMessage();
        }
      };
    } else {
      throw new RuntimeException("unsupported engine: " + opts.rpcEngine);
    }
  }

  public static void main(String []args) throws Exception {
    int rc = ToolRunner.run(new RPCCallBenchmark(), args);
    System.exit(rc);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
