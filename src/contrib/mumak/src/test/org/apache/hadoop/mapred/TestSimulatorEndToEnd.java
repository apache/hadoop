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
package org.apache.hadoop.mapred;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.MockSimulatorEngine;
import org.apache.hadoop.tools.rumen.ZombieCluster;
import org.apache.hadoop.tools.rumen.ZombieJobProducer;
import org.apache.hadoop.util.ToolRunner;

import org.junit.Test;

public class TestSimulatorEndToEnd {

  public static final Log LOG = LogFactory.getLog(MockSimulatorEngine.class);
  
  @Test
  public void testMain() throws Exception {
    final Configuration conf = new Configuration();
    final FileSystem lfs = FileSystem.getLocal(conf);
    final Path rootInputDir = new Path(
        System.getProperty("src.test.data", "data")).makeQualified(lfs);
    final Path traceFile = new Path(rootInputDir, "19-jobs.trace.json.gz");
    final Path topologyFile = new Path(rootInputDir, "19-jobs.topology.json.gz");

    LOG.info("traceFile = " + traceFile.toString() + " topology = "
        + topologyFile.toString());
   
    int numJobs = getNumberJobs(traceFile, conf);
    int nTrackers = getNumberTaskTrackers(topologyFile, conf);
    
    MockSimulatorEngine mockMumak = new MockSimulatorEngine(numJobs, nTrackers);

    String[] args = { traceFile.toString(), topologyFile.toString() };
    int res = ToolRunner.run(new Configuration(), mockMumak, args);
    Assert.assertEquals(res, 0);
  }
  
  private int getNumberJobs(Path inputFile, Configuration conf)
      throws IOException {
    ZombieJobProducer jobProducer = new ZombieJobProducer(inputFile, null, conf);
    try {
      int numJobs = 0;
      while (jobProducer.getNextJob() != null) {
        ++numJobs;
      }
      return numJobs;
    } finally {
      jobProducer.close();
    }
  }
  
  private int getNumberTaskTrackers(Path inputFile, Configuration conf)
      throws IOException {
    return new ZombieCluster(inputFile, null, conf).getMachines().size();
  }
}

