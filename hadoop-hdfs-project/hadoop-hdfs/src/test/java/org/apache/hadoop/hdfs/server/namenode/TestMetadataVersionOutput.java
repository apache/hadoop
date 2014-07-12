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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

public class TestMetadataVersionOutput {

  private MiniDFSCluster dfsCluster = null;
  private final Configuration conf = new Configuration();

  @Before
  public void setUp() throws Exception {
    dfsCluster = new MiniDFSCluster.Builder(conf).
            numDataNodes(1).
            checkExitOnShutdown(false).
            build();
    dfsCluster.waitClusterUp();
  }

  @After
  public void tearDown() throws Exception {
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
    Thread.sleep(2000);
  }

  @Test(timeout = 30000)
  public void testMetadataVersionOutput() throws IOException {

    final PrintStream origOut = System.out;
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final PrintStream stdOut = new PrintStream(baos);
    System.setOut(stdOut);
    try {
      NameNode.createNameNode(new String[] { "-metadataVersion" }, conf);
    } catch (Exception e) {
      assertExceptionContains("ExitException", e);
    }
    /* Check if meta data version is printed correctly. */
    final String verNumStr = HdfsConstants.NAMENODE_LAYOUT_VERSION + "";
    assertTrue(baos.toString("UTF-8").
      contains("HDFS Image Version: " + verNumStr));
    assertTrue(baos.toString("UTF-8").
      contains("Software format version: " + verNumStr));
    System.setOut(origOut);
  }
}
