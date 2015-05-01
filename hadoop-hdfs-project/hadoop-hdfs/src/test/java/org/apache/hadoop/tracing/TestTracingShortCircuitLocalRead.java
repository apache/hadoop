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

import static org.junit.Assume.assumeTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.htrace.Sampler;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.IOException;

public class TestTracingShortCircuitLocalRead {
  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem dfs;
  private static SpanReceiverHost spanReceiverHost;
  private static TemporarySocketDirectory sockDir;
  static final Path TEST_PATH = new Path("testShortCircuitTraceHooks");
  static final int TEST_LENGTH = 1234;

  @BeforeClass
  public static void init() {
    sockDir = new TemporarySocketDirectory();
    DomainSocket.disableBindPathValidation();
  }

  @AfterClass
  public static void shutdown() throws IOException {
    sockDir.close();
  }

  @Test
  public void testShortCircuitTraceHooks() throws IOException {
    assumeTrue(NativeCodeLoader.isNativeCodeLoaded() && !Path.WINDOWS);
    conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_CLIENT_HTRACE_PREFIX +
        SpanReceiverHost.SPAN_RECEIVERS_CONF_SUFFIX,
        TestTracing.SetSpanReceiver.class.getName());
    conf.setLong("dfs.blocksize", 100 * 1024);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY, false);
    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
        "testShortCircuitTraceHooks._PORT");
    conf.set(DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY, "CRC32C");
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .build();
    dfs = cluster.getFileSystem();

    try {
      DFSTestUtil.createFile(dfs, TEST_PATH, TEST_LENGTH, (short)1, 5678L);

      TraceScope ts = Trace.startSpan("testShortCircuitTraceHooks", Sampler.ALWAYS);
      FSDataInputStream stream = dfs.open(TEST_PATH);
      byte buf[] = new byte[TEST_LENGTH];
      IOUtils.readFully(stream, buf, 0, TEST_LENGTH);
      stream.close();
      ts.close();

      String[] expectedSpanNames = {
        "OpRequestShortCircuitAccessProto",
        "ShortCircuitShmRequestProto"
      };
      TestTracing.assertSpanNamesFound(expectedSpanNames);
    } finally {
      dfs.close();
      cluster.shutdown();
    }
  }
}
