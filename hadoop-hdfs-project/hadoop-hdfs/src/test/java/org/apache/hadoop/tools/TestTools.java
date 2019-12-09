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

package org.apache.hadoop.tools;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.hdfs.tools.DelegationTokenFetcher;
import org.apache.hadoop.hdfs.tools.JMXGet;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.ExitUtil.ExitException;
import org.junit.BeforeClass;
import org.junit.Test;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;

public class TestTools {

  private static final int PIPE_BUFFER_SIZE = 1024 * 5;
  private final static String INVALID_OPTION = "-invalidOption";
  private static final String[] OPTIONS = new String[2];

  @BeforeClass
  public static void before() {
    ExitUtil.disableSystemExit();
    OPTIONS[1] = INVALID_OPTION;
  }

  @Test  
  public void testDelegationTokenFetcherPrintUsage() {
    String pattern = "Options:";
    checkOutput(new String[] { "-help" }, pattern, System.out,
        DelegationTokenFetcher.class);
  }

  @Test  
  public void testDelegationTokenFetcherErrorOption() {
    String pattern = "ERROR: Only specify cancel, renew or print.";
    checkOutput(new String[] { "-cancel", "-renew" }, pattern, System.err,
        DelegationTokenFetcher.class);
  }

  @Test  
  public void testJMXToolHelp() {
    String pattern = "usage: jmxget options are:";
    checkOutput(new String[] { "-help" }, pattern, System.out, JMXGet.class);
  }

  @Test  
  public void testJMXToolAdditionParameter() {
    String pattern = "key = -addition";
    checkOutput(new String[] { "-service=NameNode", "-server=localhost",
        "-addition" }, pattern, System.err, JMXGet.class);
  }

  @Test
  public void testDFSAdminInvalidUsageHelp() {
    ImmutableSet<String> args = ImmutableSet.of("-report", "-saveNamespace",
        "-rollEdits", "-restoreFailedStorage", "-refreshNodes",
        "-finalizeUpgrade", "-metasave", "-refreshUserToGroupsMappings",
        "-printTopology", "-refreshNamenodes", "-deleteBlockPool",
        "-setBalancerBandwidth", "-fetchImage");
    try {
      for (String arg : args)
        assertTrue(ToolRunner.run(new DFSAdmin(), fillArgs(arg)) == -1);
      
      assertTrue(ToolRunner.run(new DFSAdmin(),
          new String[] { "-help", "-some" }) == 0);
    } catch (Exception e) {
      fail("testDFSAdminHelp error" + e);
    }

    String pattern = "Usage: hdfs dfsadmin";
    checkOutput(new String[] { "-cancel", "-renew" }, pattern, System.err,
        DFSAdmin.class);
  }

  private static String[] fillArgs(String arg) {
    OPTIONS[0] = arg;
    return OPTIONS;
  }

  private void checkOutput(String[] args, String pattern, PrintStream out,
      Class<?> clazz) {       
    ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    PrintStream oldOut = System.out;
    PrintStream oldErr = System.err;
    try {
      PipedOutputStream pipeOut = new PipedOutputStream();
      PipedInputStream pipeIn = new PipedInputStream(pipeOut, PIPE_BUFFER_SIZE);
      if (out == System.out) {
        System.setOut(new PrintStream(pipeOut));
      } else if (out == System.err) {
        System.setErr(new PrintStream(pipeOut));
      }

      if (clazz == DelegationTokenFetcher.class) {
        expectDelegationTokenFetcherExit(args);
      } else if (clazz == JMXGet.class) {
        expectJMXGetExit(args);
      } else if (clazz == DFSAdmin.class) {
        expectDfsAdminPrint(args);
      }
      pipeOut.close();
      ByteStreams.copy(pipeIn, outBytes);      
      pipeIn.close();
      assertTrue(new String(outBytes.toByteArray()).contains(pattern));            
    } catch (Exception ex) {
      fail("checkOutput error " + ex);
    } finally {
      System.setOut(oldOut);
      System.setErr(oldErr);
    }
  }

  private void expectDfsAdminPrint(String[] args) {
    try {
      ToolRunner.run(new DFSAdmin(), args);
    } catch (Exception ex) {
      fail("expectDelegationTokenFetcherExit ex error " + ex);
    }
  }

  private static void expectDelegationTokenFetcherExit(String[] args) {
    try {
      DelegationTokenFetcher.main(args);
      fail("should call exit");
    } catch (ExitException e) {
      ExitUtil.resetFirstExitException();
    } catch (Exception ex) {
      fail("expectDelegationTokenFetcherExit ex error " + ex);
    }
  }

  private static void expectJMXGetExit(String[] args) {
    try {
      JMXGet.main(args);
      fail("should call exit");
    } catch (ExitException e) {
      ExitUtil.resetFirstExitException();
    } catch (Exception ex) {
      fail("expectJMXGetExit ex error " + ex);
    }
  }
}
