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

package org.apache.hadoop.fs.statistics;

import java.util.Iterator;
import java.util.Map;

import org.junit.Test;

import org.apache.hadoop.fs.statistics.impl.EmptyIOStatistics;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertIOStatisticsAttributeNotFound;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertIOStatisticsHasAttribute;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertStatisticIsTracked;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertStatisticIsUnknown;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertStatisticIsUntracked;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test handling of the empty IO statistics class.
 */
public class TestEmptyIOStatistics extends AbstractHadoopTestBase {

  private final IOStatistics stats = EmptyIOStatistics.getInstance();

  @Test
  public void testAttributes() throws Throwable {
    assertIOStatisticsHasAttribute(stats, IOStatistics.Attributes.Static);
    assertIOStatisticsAttributeNotFound(stats, IOStatistics.Attributes.Dynamic);
    assertIOStatisticsAttributeNotFound(stats, IOStatistics.Attributes.Snapshotted);
  }

  @Test
  public void testSnapshotUnsupported() throws Throwable {
    assertThat(stats.snapshot())
        .describedAs("Snapshot of %s", stats)
        .isFalse();
  }

  @Test
  public void testIterator() throws Throwable {
    Iterator<Map.Entry<String, Long>> iterator = stats.iterator();

    assertThat(iterator.hasNext())
        .describedAs("iterator.hasNext()")
        .isFalse();
    assertThatThrownBy(iterator::next);
  }

  @Test
  public void testUnknownStatistic() throws Throwable {
    assertStatisticIsUnknown(stats, "anything");
    assertStatisticIsUntracked(stats, "anything");

    // These actually test the assertion coverage
    assertThatThrownBy(() ->
        assertStatisticIsTracked(stats, "anything"));
    assertThatThrownBy(() ->
        verifyStatisticValue(stats, "anything", 0));
  }


}
