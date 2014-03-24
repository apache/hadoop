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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHdfsAdmin {
  
  private static final Path TEST_PATH = new Path("/test");
  private final Configuration conf = new Configuration();
  private MiniDFSCluster cluster;
  
  @Before
  public void setUpCluster() throws IOException {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
  }
  
  @After
  public void shutDownCluster() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Test that we can set and clear quotas via {@link HdfsAdmin}.
   */
  @Test
  public void testHdfsAdminSetQuota() throws Exception {
    HdfsAdmin dfsAdmin = new HdfsAdmin(
        FileSystem.getDefaultUri(conf), conf);
    FileSystem fs = null;
    try {
      fs = FileSystem.get(conf);
      assertTrue(fs.mkdirs(TEST_PATH));
      assertEquals(-1, fs.getContentSummary(TEST_PATH).getQuota());
      assertEquals(-1, fs.getContentSummary(TEST_PATH).getSpaceQuota());
      
      dfsAdmin.setSpaceQuota(TEST_PATH, 10);
      assertEquals(-1, fs.getContentSummary(TEST_PATH).getQuota());
      assertEquals(10, fs.getContentSummary(TEST_PATH).getSpaceQuota());
      
      dfsAdmin.setQuota(TEST_PATH, 10);
      assertEquals(10, fs.getContentSummary(TEST_PATH).getQuota());
      assertEquals(10, fs.getContentSummary(TEST_PATH).getSpaceQuota());
      
      dfsAdmin.clearSpaceQuota(TEST_PATH);
      assertEquals(10, fs.getContentSummary(TEST_PATH).getQuota());
      assertEquals(-1, fs.getContentSummary(TEST_PATH).getSpaceQuota());
      
      dfsAdmin.clearQuota(TEST_PATH);
      assertEquals(-1, fs.getContentSummary(TEST_PATH).getQuota());
      assertEquals(-1, fs.getContentSummary(TEST_PATH).getSpaceQuota());
    } finally {
      if (fs != null) {
        fs.close();
      }
    }
  }
  
  /**
   * Make sure that a non-HDFS URI throws a helpful error.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testHdfsAdminWithBadUri() throws IOException, URISyntaxException {
    new HdfsAdmin(new URI("file:///bad-scheme"), conf);
  }
}
