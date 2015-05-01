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
package org.apache.hadoop.tracing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

public class TestTraceAdmin {
  private static final String NEWLINE = System.getProperty("line.separator");

  private String runTraceCommand(TraceAdmin trace, String... cmd)
      throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    PrintStream oldStdout = System.out;
    PrintStream oldStderr = System.err;
    System.setOut(ps);
    System.setErr(ps);
    int ret = -1;
    try {
      ret = trace.run(cmd);
    } finally {
      System.out.flush();
      System.setOut(oldStdout);
      System.setErr(oldStderr);
    }
    return "ret:" + ret + ", " + baos.toString();
  }

  private String getHostPortForNN(MiniDFSCluster cluster) {
    return "127.0.0.1:" + cluster.getNameNodePort();
  }

  @Test
  public void testCreateAndDestroySpanReceiver() throws Exception {
    Configuration conf = new Configuration();
    conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_SERVER_HTRACE_PREFIX  +
        SpanReceiverHost.SPAN_RECEIVERS_CONF_SUFFIX, "");
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    TemporarySocketDirectory tempDir = new TemporarySocketDirectory();
    String tracePath =
        new File(tempDir.getDir(), "tracefile").getAbsolutePath();
    try {
      TraceAdmin trace = new TraceAdmin();
      trace.setConf(conf);
      Assert.assertEquals("ret:0, [no span receivers found]" + NEWLINE,
          runTraceCommand(trace, "-list", "-host", getHostPortForNN(cluster)));
      Assert.assertEquals("ret:0, Added trace span receiver 1 with " +
          "configuration local-file-span-receiver.path = " + tracePath + NEWLINE,
          runTraceCommand(trace, "-add", "-host", getHostPortForNN(cluster),
              "-class", "org.apache.htrace.impl.LocalFileSpanReceiver",
              "-Clocal-file-span-receiver.path=" + tracePath));
      String list =
          runTraceCommand(trace, "-list", "-host", getHostPortForNN(cluster));
      Assert.assertTrue(list.startsWith("ret:0"));
      Assert.assertTrue(list.contains("1   org.apache.htrace.impl.LocalFileSpanReceiver"));
      Assert.assertEquals("ret:0, Removed trace span receiver 1" + NEWLINE,
          runTraceCommand(trace, "-remove", "1", "-host",
              getHostPortForNN(cluster)));
      Assert.assertEquals("ret:0, [no span receivers found]" + NEWLINE,
          runTraceCommand(trace, "-list", "-host", getHostPortForNN(cluster)));
      Assert.assertEquals("ret:0, Added trace span receiver 2 with " +
          "configuration local-file-span-receiver.path = " + tracePath + NEWLINE,
          runTraceCommand(trace, "-add", "-host", getHostPortForNN(cluster),
              "-class", "LocalFileSpanReceiver",
              "-Clocal-file-span-receiver.path=" + tracePath));
      Assert.assertEquals("ret:0, Removed trace span receiver 2" + NEWLINE,
          runTraceCommand(trace, "-remove", "2", "-host",
              getHostPortForNN(cluster)));
    } finally {
      cluster.shutdown();
      tempDir.close();
    }
  }
}
