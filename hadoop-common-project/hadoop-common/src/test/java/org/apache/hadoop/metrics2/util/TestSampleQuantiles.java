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

package org.apache.hadoop.metrics2.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.metrics2.lib.MutableInverseQuantiles;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSampleQuantiles {

  static final Quantile[] quantiles = { new Quantile(0.50, 0.050),
      new Quantile(0.75, 0.025), new Quantile(0.90, 0.010),
      new Quantile(0.95, 0.005), new Quantile(0.99, 0.001) };

  SampleQuantiles estimator;
  final static int NUM_REPEATS = 10;

  @Before
  public void init() {
    estimator = new SampleQuantiles(quantiles);
  }

  /**
   * Check that the counts of the number of items in the window and sample are
   * incremented correctly as items are added.
   */
  @Test
  public void testCount() throws IOException {
    // Counts start off zero
    assertThat(estimator.getCount()).isZero();
    assertThat(estimator.getSampleCount()).isZero();
    
    // Snapshot should be null if there are no entries.
    assertThat(estimator.snapshot()).isNull();

    // Count increment correctly by 1
    estimator.insert(1337);
    assertThat(estimator.getCount()).isOne();
    estimator.snapshot();
    assertThat(estimator.getSampleCount()).isOne();
    
    assertThat(estimator.toString()).isEqualTo(
        "50.00 %ile +/- 5.00%: 1337\n" +
        "75.00 %ile +/- 2.50%: 1337\n" +
        "90.00 %ile +/- 1.00%: 1337\n" +
        "95.00 %ile +/- 0.50%: 1337\n" +
        "99.00 %ile +/- 0.10%: 1337");
  }

  /**
   * Check that counts and quantile estimates are correctly reset after a call
   * to {@link SampleQuantiles#clear()}.
   */
  @Test
  public void testClear() throws IOException {
    for (int i = 0; i < 1000; i++) {
      estimator.insert(i);
    }
    estimator.clear();
    assertThat(estimator.getCount()).isZero();
    assertThat(estimator.getSampleCount()).isZero();
    assertThat(estimator.snapshot()).isNull();
  }

  /**
   * Correctness test that checks that absolute error of the estimate is within
   * specified error bounds for some randomly permuted streams of items.
   */
  @Test
  public void testQuantileError() throws IOException {
    final int count = 100000;
    Random rnd = new Random(0xDEADDEAD);
    int[] values = new int[count];
    for (int i = 0; i < count; i++) {
      values[i] = i + 1;
    }

    // Repeat shuffle/insert/check cycles 10 times
    for (int i = 0; i < NUM_REPEATS; i++) {

      // Shuffle
      Collections.shuffle(Arrays.asList(values), rnd);
      estimator.clear();

      // Insert
      for (int value : values) {
        estimator.insert(value);
      }
      Map<Quantile, Long> snapshot;
      snapshot = estimator.snapshot();

      // Check
      for (Quantile q : quantiles) {
        long actual = (long) (q.quantile * count);
        long error = (long) (q.error * count);
        long estimate = snapshot.get(q);
        assertThat(estimate <= actual + error).isTrue();
        assertThat(estimate >= actual - error).isTrue();
      }
    }
  }

  /**
   * Correctness test that checks that absolute error of the estimate for inverse quantiles
   * is within specified error bounds for some randomly permuted streams of items.
   */
  @Test
  public void testInverseQuantiles() throws IOException {
    SampleQuantiles inverseQuantilesEstimator =
        new SampleQuantiles(MutableInverseQuantiles.INVERSE_QUANTILES);
    final int count = 100000;
    Random rnd = new Random(0xDEADDEAD);
    int[] values = new int[count];
    for (int i = 0; i < count; i++) {
      values[i] = i + 1;
    }

    // Repeat shuffle/insert/check cycles 10 times
    for (int i = 0; i < NUM_REPEATS; i++) {
      // Shuffle
      Collections.shuffle(Arrays.asList(values), rnd);
      inverseQuantilesEstimator.clear();

      // Insert
      for (int value : values) {
        inverseQuantilesEstimator.insert(value);
      }
      Map<Quantile, Long> snapshot;
      snapshot = inverseQuantilesEstimator.snapshot();

      // Check
      for (Quantile q : MutableInverseQuantiles.INVERSE_QUANTILES) {
        long actual = (long) (q.quantile * count);
        long error = (long) (q.error * count);
        long estimate = snapshot.get(q);
        assertThat(estimate <= actual + error).isTrue();
        assertThat(estimate >= actual - error).isTrue();
      }
    }
  }
}
