/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.junit.Test;

import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

public class TestDataNodeFSDataSetSink {
  private static final MetricsSystemImpl ms = new
    MetricsSystemImpl("TestFSDataSet");

  class FSDataSetSinkTest implements MetricsSink {
    private Set<String> nameMap;
    private int count;

    /**
     * add a metrics record in the sink
     *
     * @param record the record to add
     */
    @Override
    public void putMetrics(MetricsRecord record) {
      // let us do this only once, otherwise
      // our count could go out of sync.
      if (count == 0) {
        for (AbstractMetric m : record.metrics()) {
          if (nameMap.contains(m.name())) {
            count++;
          }
        }

        for (MetricsTag t : record.tags()) {
          if (nameMap.contains(t.name())) {
            count++;
          }
        }
      }
    }

    /**
     * Flush any buffered metrics
     */
    @Override
    public void flush() {

    }

    /**
     * Initialize the plugin
     *
     * @param conf the configuration object for the plugin
     */
    @Override
    public void init(SubsetConfiguration conf) {
      nameMap = new TreeSet<>();
      nameMap.add("DfsUsed");
      nameMap.add("Capacity");
      nameMap.add("Remaining");
      nameMap.add("StorageInfo");
      nameMap.add("NumFailedVolumes");
      nameMap.add("LastVolumeFailureDate");
      nameMap.add("EstimatedCapacityLostTotal");
      nameMap.add("CacheUsed");
      nameMap.add("CacheCapacity");
      nameMap.add("NumBlocksCached");
      nameMap.add("NumBlocksFailedToCache");
      nameMap.add("NumBlocksFailedToUnCache");
      nameMap.add("Context");
      nameMap.add("Hostname");
    }

    public int getMapCount() {
      return nameMap.size();
    }

    public int getFoundKeyCount() {
      return count;
    }
  }

  @Test
  /**
   * This test creates a Source and then calls into the Sink that we
   * have registered. That is calls into FSDataSetSinkTest
   */
  public void testFSDataSetMetrics() throws InterruptedException {
    Configuration conf = new HdfsConfiguration();
    String bpid = "FSDatSetSink-Test";
    SimulatedFSDataset fsdataset = new SimulatedFSDataset(null, conf);
    fsdataset.addBlockPool(bpid, conf);
    FSDataSetSinkTest sink = new FSDataSetSinkTest();
    sink.init(null);
    ms.init("Test");
    ms.start();
    ms.register("FSDataSetSource", "FSDataSetSource", fsdataset);
    ms.register("FSDataSetSink", "FSDataSetSink", sink);
    ms.startMetricsMBeans();
    ms.publishMetricsNow();

    Thread.sleep(4000);

    ms.stopMetricsMBeans();
    ms.shutdown();

    // make sure we got all expected metric in the call back
    assertEquals(sink.getMapCount(), sink.getFoundKeyCount());

  }
}
