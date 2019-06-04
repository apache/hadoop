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

package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Unit tests for HBaseTimelineStorageUtils static methos.
 */
public class TestHBaseTimelineStorageUtils {

  private String hbaseConfigPath = "target/hbase-site.xml";

  @Before
  public void setup() throws IOException {
    // Input Hbase Configuration
    Configuration hbaseConf = new Configuration();
    hbaseConf.set("input", "test");

    //write the document to a buffer (not directly to the file, as that
    //can cause the file being written to get read which will then fail.
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    hbaseConf.writeXml(bytesOut);
    bytesOut.close();

    //write the bytes to the file
    File file = new File(hbaseConfigPath);
    OutputStream os = new FileOutputStream(file);
    os.write(bytesOut.toByteArray());
    os.close();
  }

  @Test(expected=NullPointerException.class)
  public void testGetTimelineServiceHBaseConfNullArgument() throws Exception {
    HBaseTimelineStorageUtils.getTimelineServiceHBaseConf(null);
  }

  @Test
  public void testWithHbaseConfAtLocalFileSystem() throws IOException {
    // Verifying With Hbase Conf from Local FileSystem
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.TIMELINE_SERVICE_HBASE_CONFIGURATION_FILE,
        hbaseConfigPath);
    Configuration hbaseConfFromLocal =
        HBaseTimelineStorageUtils.getTimelineServiceHBaseConf(conf);
    Assert.assertEquals("Failed to read hbase config from Local FileSystem",
        "test", hbaseConfFromLocal.get("input"));
  }

  @Test
  public void testWithHbaseConfAtHdfsFileSystem() throws IOException {
    MiniDFSCluster hdfsCluster = null;
    try {
      HdfsConfiguration hdfsConfig = new HdfsConfiguration();
      hdfsCluster = new MiniDFSCluster.Builder(hdfsConfig)
          .numDataNodes(1).build();

      FileSystem fs = hdfsCluster.getFileSystem();
      Path path = new Path("/tmp/hdfs-site.xml");
      fs.copyFromLocalFile(new Path(hbaseConfigPath), path);

      // Verifying With Hbase Conf from HDFS FileSystem
      Configuration conf = new Configuration(hdfsConfig);
      conf.set(YarnConfiguration.TIMELINE_SERVICE_HBASE_CONFIGURATION_FILE,
          path.toString());
      Configuration hbaseConfFromHdfs =
          HBaseTimelineStorageUtils.getTimelineServiceHBaseConf(conf);
      Assert.assertEquals("Failed to read hbase config from Hdfs FileSystem",
          "test", hbaseConfFromHdfs.get("input"));
    } finally {
      if (hdfsCluster != null) {
        hdfsCluster.shutdown();
      }
    }
  }

}
