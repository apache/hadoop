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

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.statistics.impl.EmptyIOStatistics;
import org.apache.hadoop.test.HadoopTestBase;

import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.*;

public class TestEmptyIOStatistics extends HadoopTestBase {

  private final IOStatistics stats = new EmptyIOStatistics();

  @Test
  public void testAttributes() throws Throwable {
    assertHasAttribute(stats, IOStatistics.Attributes.Static);
    assertDoesNotHaveAttribute(stats, IOStatistics.Attributes.Dynamic);
    assertDoesNotHaveAttribute(stats, IOStatistics.Attributes.Snapshotted);
  }

  @Test
  public void testSnapshotUnsupported() throws Throwable {
    Assertions.assertThat(stats.snapshot())
        .describedAs("Snapshot of %s", stats)
        .isFalse();
  }

  @Test
  public void testIterator() throws Throwable {
    Iterator<Map.Entry<String, Long>> iterator = stats.iterator();

    Assertions.assertThat(iterator.hasNext())
        .describedAs("iterator.hasNext()")
        .isFalse();
    Assertions.assertThatThrownBy(iterator::next);
  }

  @Test
  public void testUnknownStatistic() throws Throwable {
    assertStatisticUnknown(stats, "anything");
    assertStatisticUntracked(stats, "anything");

    // These actually test the assertion coverage
    Assertions.assertThatThrownBy(() ->
        assertStatisticTracked(stats, "anything"));
    Assertions.assertThatThrownBy(() ->
        assertStatisticsValue(stats, "anything", 0));
  }


}
