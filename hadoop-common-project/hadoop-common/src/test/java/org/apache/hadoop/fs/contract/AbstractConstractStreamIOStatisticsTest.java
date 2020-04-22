/*
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

package org.apache.hadoop.fs.contract;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatistics.Attributes;

import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.*;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_BYTES;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_WRITE_BYTES;

/**
 * Tests {@link IOStatistics} support in input streams.
 * Requires both the input and output streams to offer statistics.
 */
public abstract class AbstractConstractStreamIOStatisticsTest
    extends AbstractFSContractTestBase {


  @Test
  public void testWriteByteStats() throws Throwable {
    describe("Write a byte to a file and verify"
        + " the stream statistics are updated");
    Path path = methodPath();
    FileSystem fs = getFileSystem();
    fs.mkdirs(path.getParent());
    try (FSDataOutputStream out = fs.create(path, true)) {
      IOStatistics statistics = extractStatistics(out);
      final Set<Attributes> attrs = outputStreamAttributes();
      IOStatistics st = statistics;
      attrs.forEach(a ->
          assertIOStatisticsHasAttribute(st, a));
      final boolean isDynamic = statistics.hasAttribute(Attributes.Dynamic);
      final List<String> keys = outputStreamStatisticKeys();
      Assertions.assertThat(keys).contains(STREAM_WRITE_BYTES);
      // before a write, no bytes
      assertStatisticHasValue(statistics, STREAM_WRITE_BYTES, 0);
      out.write('0');
      if (!isDynamic) {
        statistics = extractStatistics(out);
      }
      assertStatisticHasValue(statistics, STREAM_WRITE_BYTES, 1);
      // close the stream
      out.close();
      // statistics are still valid
      statistics = extractStatistics(out);
      assertStatisticHasValue(statistics, STREAM_WRITE_BYTES, 1);
    } finally {
      fs.delete(path, false);
    }
  }


  /**
   * Keys which the output stream must support.
   * @return a list of keys
   */
  public List<String> outputStreamStatisticKeys() {
    return Arrays.asList(STREAM_WRITE_BYTES);
  }

  /**
   * Attributes of the output stream's statistics.
   * @return all attributes which are expected.
   */
  public Set<Attributes> outputStreamAttributes() {
    return EnumSet.of(Attributes.Dynamic);
  }

  /**
   * Attributes of the input stream's statistics.
   * @return all attributes which are expected.
   */
  public Set<Attributes> inputStreamAttributes() {
    return EnumSet.of(Attributes.Dynamic);
  }

  /**
   * Keys which the output stream must support.
   * @return a list of keys
   */
  public List<String> intputStreamStatisticKeys() {
    return Arrays.asList(STREAM_READ_BYTES);
  }

}
