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

package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

public class TestDataNodeNetworkErrorsWithDefaultConf {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDataNodeNetworkErrorsWithDefaultConf.class);

  @Test(timeout = 60000)
  public void testDatanodeNetworkErrorsMetricDefaultConf() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(6).build();
    cluster.waitActive();
    final List<FSDataOutputStream> streams = Lists.newArrayList();
    DataNodeFaultInjector oldInjector = DataNodeFaultInjector.get();
    DataNodeFaultInjector newInjector = new DataNodeFaultInjector() {
      public void incrementDatanodeNetworkErrors(DataXceiver dataXceiver) {
        dataXceiver.incrDatanodeNetworkErrorsWithPort();
      }
    };
    DataNodeFaultInjector.set(newInjector);
    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          try {
            for (int i = 0; i < 100; i++) {
              final Path path = new Path("/test" + i);
              final FSDataOutputStream out =
                  cluster.getFileSystem().create(path, (short) 3);
              streams.add(out);
              out.writeBytes("old gs data\n");
              out.hflush();
            }
          } catch (IOException e) {
            e.printStackTrace();
          }

          final MetricsRecordBuilder dnMetrics =
              getMetrics(cluster.getDataNodes().get(0).getMetrics().name());
          long datanodeNetworkErrors = getLongCounter("DatanodeNetworkErrors", dnMetrics);
          return datanodeNetworkErrors > 10;
        }
      }, 1000, 60000);

      final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      final ObjectName mxbeanName =
          new ObjectName("Hadoop:service=DataNode,name=DataNodeInfo");
      final Object dnc =
          mbs.getAttribute(mxbeanName, "DatanodeNetworkCounts");
      // Compute number of DatanodeNetworkCounts.
      final String allDnc = dnc.toString();
      int oldStringLength = allDnc.length();
      String keyword = "key=networkErrors, value";
      int newStringLength = allDnc.replace(keyword, "").length();
      int networkErrorsCount = (oldStringLength - newStringLength) / keyword.length();
      final MetricsRecordBuilder dnMetrics =
          getMetrics(cluster.getDataNodes().get(0).getMetrics().name());
      long datanodeNetworkErrors = getLongCounter("DatanodeNetworkErrors", dnMetrics);
      Assert.assertEquals(datanodeNetworkErrors, networkErrorsCount);
    } finally {
      IOUtils.cleanupWithLogger(LOG, streams.toArray(new Closeable[0]));
      if (cluster != null) {
        cluster.shutdown();
      }
      DataNodeFaultInjector.set(oldInjector);
    }
  }
}
