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

package org.apache.hadoop.fs.statistics.impl;

import org.junit.Test;

import org.apache.hadoop.fs.statistics.IOStatisticEntry;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.statistics.impl.IOStatisticsImplementationUtils.aggregate;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the IOStatisticEntry class and util methods in
 * the statistics.impl package.
 */
public class TestIOStatisticEntry extends AbstractHadoopTestBase {

  /**
   * This is a type we know is not in the list of known types,
   * so lacks an aggregator or any rule about aggregation
   */
  public static final int X = 10;

  private final IOStatisticEntry counter1 = (Long) (long) 1;

  private final IOStatisticEntry counter2 = (Long) (long) 2;

  private final IOStatisticEntry min1 = (Long) (long) 1;

  private final IOStatisticEntry min2 = (Long) (long) 2;

  private final IOStatisticEntry max1 = (Long) (long) 1;

  private final IOStatisticEntry max2 = (Long) (long) 2;

  private final IOStatisticEntry mean1 = (Long) (long) 1;

  private final IOStatisticEntry mean12 = (Long) (long) 2;

  private final IOStatisticEntry mean2 = (Long) (long) 10;

  @Test
  public void testCounterAdd() throws Throwable {
    assertThat(aggregate(counter1, counter1))
        .isEqualTo(counter2);
  }

  @Test
  public void testMinAggregate() throws Throwable {
    assertThat(aggregate(min1, min2))
        .isEqualTo(min1);
  }

  @Test
  public void testMaxAggregate() throws Throwable {
    assertThat(aggregate(max1, max2))
        .isEqualTo(max2);
  }

  @Test
  public void testMeanAggregate1() throws Throwable {
    assertThat(aggregate(mean1, mean1))
        .isEqualTo(mean12);
  }

  @Test
  public void testMeanAggregateRounding() throws Throwable {
    assertThat(aggregate(mean1, mean2))
        .isEqualTo((long) 11);
  }

  /**
   * Unknown types are aggregated just by taking
   * the left value.
   */
  @Test
  public void testUnknownTypeAggregation() throws Throwable {
    IOStatisticEntry e1 = (Long) (long) 1;
    IOStatisticEntry e2 = (Long) (long) 2;
    assertThat(aggregate(e1, e2))
        .isEqualTo(e1);
  }

  @Test
  public void testAccessors() throws Throwable {
    IOStatisticEntry e1 = (Long) (long) 4;
    assertThat(e1)
        .extracting(IOStatisticEntry::_1)
        .isEqualTo(1L);
    assertThat(e1)
        .extracting(IOStatisticEntry::_2)
        .isEqualTo(2L);
    assertThat(e1)
        .extracting(IOStatisticEntry::_3)
        .isEqualTo(3L);
    assertThat(e1.arity())
        .isEqualTo(4);
    assertThat(e1.typeAsString())
        .isEqualTo("10");
    long[] data = e1.getData();
    assertThat(data.length)
        .isEqualTo(e1.arity())
        .isEqualTo(4);
  }

  @Test
  public void testStringAccessors() throws Throwable {
   assertThat(counter1.typeAsString())
        .isEqualTo("counter");
    assertThat(min1.typeAsString())
        .isEqualTo("min");
    assertThat(max1.typeAsString())
        .isEqualTo("max");
    assertThat(mean1.typeAsString())
        .isEqualTo("mean");
  }

  @Test
  public void testWrongArity() throws Throwable {
    intercept(IllegalArgumentException.class, () -> (long) 1);
    intercept(IllegalArgumentException.class, () -> (long) 1);
  }

  @Test
  public void testNoData() throws Throwable {
    intercept(IllegalArgumentException.class, () -> (long) 0);
  }
}
