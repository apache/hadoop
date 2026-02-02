/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.compat.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.fs.compat.HdfsCompatTool;
import org.apache.hadoop.fs.compat.hdfs.HdfsCompatMiniCluster;
import org.apache.hadoop.fs.compat.hdfs.HdfsCompatTestCommand;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class TestHdfsCompatDefaultSuites {
  @Test
  public void testSuiteAll() throws Exception {
    HdfsCompatMiniCluster cluster = new HdfsCompatMiniCluster();
    try {
      cluster.start();
      final String uri = cluster.getUri() + "/tmp";
      Configuration conf = cluster.getConf();
      HdfsCompatCommand cmd = new HdfsCompatTestCommand(uri, "ALL", conf);
      cmd.initialize();
      HdfsCompatReport report = cmd.apply();
      assertEquals(0, report.getFailedCase().size());
      new HdfsCompatTool(conf).printReport(report, System.out);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testSuiteTpcds() throws Exception {
    HdfsCompatMiniCluster cluster = new HdfsCompatMiniCluster();
    try {
      cluster.start();
      final String uri = cluster.getUri() + "/tmp";
      Configuration conf = cluster.getConf();
      HdfsCompatCommand cmd = new HdfsCompatTestCommand(uri, "TPCDS", conf);
      cmd.initialize();
      HdfsCompatReport report = cmd.apply();
      assertEquals(0, report.getFailedCase().size());
      new HdfsCompatTool(conf).printReport(report, System.out);
    } finally {
      cluster.shutdown();
    }
  }
}