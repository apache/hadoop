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

import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This test extends {@link TestReconstructStripedFile} to test
 * ec reconstruction validation.
 */
public class TestReconstructStripedFileWithValidator
    extends TestReconstructStripedFile {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestReconstructStripedFileWithValidator.class);

  public TestReconstructStripedFileWithValidator() {
    LOG.info("run {} with validator.",
        TestReconstructStripedFileWithValidator.class.getSuperclass()
            .getSimpleName());
  }

  /**
   * This test injects data pollution into decoded outputs once.
   * When validation enabled, the first reconstruction task should fail
   * in the validation, but the data will be recovered correctly
   * by the next task.
   * On the other hand, when validation disabled, the first reconstruction task
   * will succeed and then lead to data corruption.
   */
  @Test(timeout = 120000)
  public void testValidatorWithBadDecoding()
      throws Exception {
    MiniDFSCluster cluster = getCluster();

    cluster.getDataNodes().stream()
        .map(DataNode::getMetrics)
        .map(DataNodeMetrics::getECInvalidReconstructionTasks)
        .forEach(n -> Assert.assertEquals(0, (long) n));

    DataNodeFaultInjector oldInjector = DataNodeFaultInjector.get();
    DataNodeFaultInjector badDecodingInjector = new DataNodeFaultInjector() {
      private final AtomicBoolean flag = new AtomicBoolean(false);

      @Override
      public void badDecoding(ByteBuffer[] outputs) {
        if (!flag.get()) {
          for (ByteBuffer output : outputs) {
            output.mark();
            output.put((byte) (output.get(output.position()) + 1));
            output.reset();
          }
        }
        flag.set(true);
      }
    };
    DataNodeFaultInjector.set(badDecodingInjector);

    int fileLen =
        (getEcPolicy().getNumDataUnits() + getEcPolicy().getNumParityUnits())
            * getBlockSize() + getBlockSize() / 10;
    try {
      assertFileBlocksReconstruction(
          "/testValidatorWithBadDecoding",
          fileLen,
          ReconstructionType.DataOnly,
          getEcPolicy().getNumParityUnits());

      long sum = cluster.getDataNodes().stream()
          .map(DataNode::getMetrics)
          .mapToLong(DataNodeMetrics::getECInvalidReconstructionTasks)
          .sum();
      Assert.assertEquals(1, sum);
    } finally {
      DataNodeFaultInjector.set(oldInjector);
    }
  }

  @Override
  public boolean isValidationEnabled() {
    return true;
  }

  /**
   * Set a small value for the failed reconstruction task to be
   * rescheduled in a short period of time.
   */
  @Override
  public int getPendingTimeout() {
    return 10;
  }
}
