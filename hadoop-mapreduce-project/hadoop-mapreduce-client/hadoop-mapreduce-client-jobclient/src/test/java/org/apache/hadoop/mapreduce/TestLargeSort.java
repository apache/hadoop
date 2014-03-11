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

package org.apache.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestLargeSort {
  MiniMRClientCluster cluster;

  @Before
  public void setup() throws IOException {
    Configuration conf = new YarnConfiguration();
    cluster = MiniMRClientClusterFactory.create(this.getClass(), 2, conf);
    cluster.start();
  }

  @After
  public void cleanup() throws IOException {
    if (cluster != null) {
      cluster.stop();
      cluster = null;
    }
  }

  @Test
  public void testLargeSort() throws Exception {
    String[] args = new String[0];
    int[] ioSortMbs = {128, 256, 1536};
    for (int ioSortMb : ioSortMbs) {
      Configuration conf = new Configuration(cluster.getConfig());
      conf.setInt(MRJobConfig.IO_SORT_MB, ioSortMb);
      conf.setInt(LargeSorter.NUM_MAP_TASKS, 1);
      conf.setInt(LargeSorter.MBS_PER_MAP, ioSortMb);
      assertEquals("Large sort failed for " + ioSortMb, 0,
          ToolRunner.run(conf, new LargeSorter(), args));
    }
  }
}
