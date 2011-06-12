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

package org.apache.hadoop.raid;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.net.InetSocketAddress;
import javax.security.auth.login.LoginException;

import org.apache.hadoop.ipc.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.raid.protocol.PolicyInfo;
import org.apache.hadoop.raid.protocol.PolicyList;
import org.apache.hadoop.raid.protocol.RaidProtocol;

/**
 * A {@link RaidShell} that allows browsing configured raid policies.
 */
public class RaidShell extends Configured implements Tool {
  public static final Log LOG = LogFactory.getLog( "org.apache.hadoop.RaidShell");
  public RaidProtocol raidnode;
  final RaidProtocol rpcRaidnode;
  private UserGroupInformation ugi;
  volatile boolean clientRunning = true;
  private Configuration conf;

  /**
   * Start RaidShell.
   * <p>
   * The RaidShell connects to the specified RaidNode and performs basic
   * configuration options.
   * @throws IOException
   */
  public RaidShell() throws IOException {
    this(new Configuration());
  }

  /**
   * The RaidShell connects to the specified RaidNode and performs basic
   * configuration options.
   * @param conf The Hadoop configuration
   * @throws IOException
   */
  public RaidShell(Configuration conf) throws IOException {
    this(conf, RaidNode.getAddress(conf));
  }

  public RaidShell(Configuration conf, InetSocketAddress address) throws IOException {
    super(conf);
    this.ugi = UserGroupInformation.getCurrentUser();

    this.rpcRaidnode = createRPCRaidnode(address, conf, ugi);
    this.raidnode = createRaidnode(rpcRaidnode);
  }
  
  public static RaidProtocol createRaidnode(Configuration conf) throws IOException {
    return createRaidnode(RaidNode.getAddress(conf), conf);
  }

  public static RaidProtocol createRaidnode(InetSocketAddress raidNodeAddr,
      Configuration conf) throws IOException {
    return createRaidnode(createRPCRaidnode(raidNodeAddr, conf,
      UserGroupInformation.getCurrentUser()));

  }

  private static RaidProtocol createRPCRaidnode(InetSocketAddress raidNodeAddr,
      Configuration conf, UserGroupInformation ugi)
    throws IOException {
    LOG.info("RaidShell connecting to " + raidNodeAddr);
    return (RaidProtocol)RPC.getProxy(RaidProtocol.class,
        RaidProtocol.versionID, raidNodeAddr, ugi, conf,
        NetUtils.getSocketFactory(conf, RaidProtocol.class));
  }

  private static RaidProtocol createRaidnode(RaidProtocol rpcRaidnode)
    throws IOException {
    RetryPolicy createPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
        5, 5000, TimeUnit.MILLISECONDS);

    Map<Class<? extends Exception>,RetryPolicy> remoteExceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();

    Map<Class<? extends Exception>,RetryPolicy> exceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();
    exceptionToPolicyMap.put(RemoteException.class,
        RetryPolicies.retryByRemoteException(
            RetryPolicies.TRY_ONCE_THEN_FAIL, remoteExceptionToPolicyMap));
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    Map<String,RetryPolicy> methodNameToPolicyMap = new HashMap<String,RetryPolicy>();

    methodNameToPolicyMap.put("create", methodPolicy);

    return (RaidProtocol) RetryProxy.create(RaidProtocol.class,
        rpcRaidnode, methodNameToPolicyMap);
  }

  private void checkOpen() throws IOException {
    if (!clientRunning) {
      IOException result = new IOException("RaidNode closed");
      throw result;
    }
  }

  /**
   * Close the connection to the raidNode.
   */
  public synchronized void close() throws IOException {
    if(clientRunning) {
      clientRunning = false;
      RPC.stopProxy(rpcRaidnode);
    }
  }

  /**
   * Displays format of commands.
   */
  private static void printUsage(String cmd) {
    String prefix = "Usage: java " + RaidShell.class.getSimpleName();
    if ("-showConfig".equals(cmd)) {
      System.err.println("Usage: java RaidShell" + 
                         " [-showConfig]"); 
    } else if ("-recover".equals(cmd)) {
      System.err.println("Usage: java CronShell" +
                         " [-recover srcPath1 corruptOffset]");
    } else {
      System.err.println("Usage: java RaidShell");
      System.err.println("           [-showConfig ]");
      System.err.println("           [-help [cmd]]");
      System.err.println("           [-recover srcPath1 corruptOffset]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }

  /**
   * run
   */
  public int run(String argv[]) throws Exception {

    if (argv.length < 1) {
      printUsage("");
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = argv[i++];
    //
    // verify that we have enough command line parameters
    //
    if ("-showConfig".equals(cmd)) {
      if (argv.length < 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-recover".equals(cmd)) {
      if (argv.length < 3) {
        printUsage(cmd);
        return exitCode;
      }
    }

    try {
      if ("-showConfig".equals(cmd)) {
        exitCode = showConfig(cmd, argv, i);
      } else if ("-recover".equals(cmd)) {
        exitCode = recoverAndPrint(cmd, argv, i);
      } else {
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": Unknown command");
        printUsage("");
      }
    } catch (IllegalArgumentException arge) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + arge.getLocalizedMessage());
      printUsage(cmd);
    } catch (RemoteException e) {
      //
      // This is a error returned by raidnode server. Print
      // out the first line of the error mesage, ignore the stack trace.
      exitCode = -1;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        System.err.println(cmd.substring(1) + ": " +
                           content[0]);
      } catch (Exception ex) {
        System.err.println(cmd.substring(1) + ": " +
                           ex.getLocalizedMessage());
      }
    } catch (IOException e) {
      //
      // IO exception encountered locally.
      // 
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " +
                         e.getLocalizedMessage());
    } catch (Exception re) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + re.getLocalizedMessage());
    } finally {
    }
    return exitCode;
  }

  /**
   * Apply operation specified by 'cmd' on all parameters
   * starting from argv[startindex].
   */
  private int showConfig(String cmd, String argv[], int startindex) throws IOException {
    int exitCode = 0;
    int i = startindex;
    PolicyList[] all = raidnode.getAllPolicies();
    for (PolicyList list: all) {
      for (PolicyInfo p : list.getAll()) {
        System.out.println(p);
      }
    }
    return exitCode;
  }

  /**
   * Recovers the specified path from the parity file
   */
  public Path[] recover(String cmd, String argv[], int startindex)
    throws IOException {
    Path[] paths = new Path[(argv.length - startindex) / 2];
    int j = 0;
    for (int i = startindex; i < argv.length; i = i + 2) {
      String path = argv[i];
      long corruptOffset = Long.parseLong(argv[i+1]);
      LOG.info("RaidShell recoverFile for " + path + " corruptOffset " + corruptOffset);
      paths[j] = new Path(raidnode.recoverFile(path, corruptOffset));
      LOG.info("Raidshell created recovery file " + paths[j]);
      j++;
    }
    return paths;
  }

  public int recoverAndPrint(String cmd, String argv[], int startindex)
    throws IOException {
    int exitCode = 0;
    for (Path p : recover(cmd,argv,startindex)) {
      System.out.println(p);
    }
    return exitCode;
  }
  
  /**
   * main() has some simple utility methods
   */
  public static void main(String argv[]) throws Exception {
    RaidShell shell = null;
    try {
      shell = new RaidShell();
    } catch (RPC.VersionMismatch v) {
      System.err.println("Version Mismatch between client and server" +
                         "... command aborted.");
      System.exit(-1);
    } catch (IOException e) {
      System.err.println("Bad connection to RaidNode. command aborted.");
      System.exit(-1);
    }

    int res;
    try {
      res = ToolRunner.run(shell, argv);
    } finally {
      shell.close();
    }
    System.exit(res);
  }
}
