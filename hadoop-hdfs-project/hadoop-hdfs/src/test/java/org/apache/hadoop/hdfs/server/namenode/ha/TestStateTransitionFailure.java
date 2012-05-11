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
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.junit.Test;

/**
 * Tests to verify the behavior of failing to fully start transition HA states.
 */
public class TestStateTransitionFailure {
  
  public static final Log LOG = LogFactory.getLog(TestStateTransitionFailure.class);

  /**
   * Ensure that a failure to fully transition to the active state causes a
   * shutdown of the NameNode.
   */
  @Test
  public void testFailureToTransitionCausesShutdown() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      // Set an illegal value for the trash emptier interval. This will cause
      // the NN to fail to transition to the active state.
      conf.setLong(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, -1);
      cluster = new MiniDFSCluster.Builder(conf)
          .nnTopology(MiniDFSNNTopology.simpleHATopology())
          .numDataNodes(0)
          .build();
      cluster.waitActive();
      Runtime mockRuntime = mock(Runtime.class);
      cluster.getNameNode(0).setRuntimeForTesting(mockRuntime);
      verify(mockRuntime, times(0)).exit(anyInt());
      try {
        cluster.transitionToActive(0);
        fail("Transitioned to active but should not have been able to.");
      } catch (ServiceFailedException sfe) {
        assertExceptionContains("Error encountered requiring NN shutdown. " +
            "Shutting down immediately.", sfe);
        LOG.info("got expected exception", sfe);
      }
      verify(mockRuntime, times(1)).exit(anyInt());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
