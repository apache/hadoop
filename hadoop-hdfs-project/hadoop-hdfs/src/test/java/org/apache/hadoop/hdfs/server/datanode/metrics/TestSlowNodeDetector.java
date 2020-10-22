/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode.metrics;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link OutlierDetector}.
 */
public class TestSlowNodeDetector {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestSlowNodeDetector.class);

  /**
   * Set a timeout for every test case.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300_000);

  private final static double LOW_THRESHOLD = 1000;
  private final static long MIN_OUTLIER_DETECTION_PEERS = 3;

  // Randomly generated test cases for median and MAD. The first entry
  // in each pair is the expected median and the second entry is the
  // expected Median Absolute Deviation. The small sets of size 1 and 2
  // exist to test the edge cases however in practice the MAD of a very
  // small set is not useful.
  private Map<List<Double>, Pair<Double, Double>> medianTestMatrix =
      new ImmutableMap.Builder<List<Double>, Pair<Double, Double>>()
          // Single element.
          .put(new ImmutableList.Builder<Double>()
                  .add(9.6502431302).build(),
              Pair.of(9.6502431302, 0.0))

          // Two elements.
          .put(new ImmutableList.Builder<Double>()
                  .add(1.72168104625)
                  .add(11.7872544459).build(),
              Pair.of(6.75446774606, 7.4616095611))

          // The Remaining lists were randomly generated with sizes 3-10.
          .put(new ImmutableList.Builder<Double>()
                  .add(76.2635686249)
                  .add(27.0652018553)
                  .add(1.3868476443)
                  .add(49.7194624164)
                  .add(47.385680883)
                  .add(57.8721199173).build(),
              Pair.of(48.5525716497, 22.837202532))

          .put(new ImmutableList.Builder<Double>()
                  .add(86.0573389581)
                  .add(93.2399572424)
                  .add(64.9545429122)
                  .add(35.8509730085)
                  .add(1.6534313654).build(),
              Pair.of(64.9545429122, 41.9360180373))

          .put(new ImmutableList.Builder<Double>()
                  .add(5.00127007366)
                  .add(37.9790589127)
                  .add(67.5784746266).build(),
              Pair.of(37.9790589127, 43.8841594039))

          .put(new ImmutableList.Builder<Double>()
                  .add(1.43442932944)
                  .add(70.6769829947)
                  .add(37.47579656)
                  .add(51.1126141394)
                  .add(72.2465914419)
                  .add(32.2930549225)
                  .add(39.677459781).build(),
              Pair.of(39.677459781, 16.9537852208))

          .put(new ImmutableList.Builder<Double>()
                  .add(26.7913745214)
                  .add(68.9833706658)
                  .add(29.3882180746)
                  .add(68.3455244453)
                  .add(74.9277265022)
                  .add(12.1469972942)
                  .add(72.5395402683)
                  .add(7.87917492506)
                  .add(33.3253447774)
                  .add(72.2753759125).build(),
              Pair.of(50.8354346113, 31.9881230079))

          .put(new ImmutableList.Builder<Double>()
                  .add(38.6482290705)
                  .add(88.0690746319)
                  .add(50.6673611649)
                  .add(64.5329814115)
                  .add(25.2580979294)
                  .add(59.6709630711)
                  .add(71.5406993741)
                  .add(81.3073035091)
                  .add(20.5549547284).build(),
              Pair.of(59.6709630711, 31.1683520683))

          .put(new ImmutableList.Builder<Double>()
                  .add(87.352734249)
                  .add(65.4760359094)
                  .add(28.9206803169)
                  .add(36.5908574008)
                  .add(87.7407653175)
                  .add(99.3704511335)
                  .add(41.3227434076)
                  .add(46.2713494909)
                  .add(3.49940920921).build(),
              Pair.of(46.2713494909, 28.4729106898))

          .put(new ImmutableList.Builder<Double>()
                  .add(95.3251533286)
                  .add(27.2777870437)
                  .add(43.73477168).build(),
              Pair.of(43.73477168, 24.3991619317))

          .build();

  // A test matrix that maps inputs to the expected output list of
  // slow nodes i.e. outliers.
  private Map<Map<String, Double>, Set<String>> outlierTestMatrix =
      new ImmutableMap.Builder<Map<String, Double>, Set<String>>()
          // The number of samples is too low and all samples are below
          // the low threshold. Nothing should be returned.
          .put(ImmutableMap.of(
              "n1", 0.0,
              "n2", LOW_THRESHOLD + 1),
              ImmutableSet.of())

          // A statistical outlier below the low threshold must not be
          // returned.
          .put(ImmutableMap.of(
              "n1", 1.0,
              "n2", 1.0,
              "n3", LOW_THRESHOLD - 1),
              ImmutableSet.of())

          // A statistical outlier above the low threshold must be returned.
          .put(ImmutableMap.of(
              "n1", 1.0,
              "n2", 1.0,
              "n3", LOW_THRESHOLD + 1),
              ImmutableSet.of("n3"))

          // A statistical outlier must not be returned if it is within a
          // MEDIAN_MULTIPLIER multiple of the median.
          .put(ImmutableMap.of(
              "n1", LOW_THRESHOLD + 0.1,
              "n2", LOW_THRESHOLD + 0.1,
              "n3", LOW_THRESHOLD * OutlierDetector.MEDIAN_MULTIPLIER - 0.1),
              ImmutableSet.of())

          // A statistical outlier must be returned if it is outside a
          // MEDIAN_MULTIPLIER multiple of the median.
          .put(ImmutableMap.of(
              "n1", LOW_THRESHOLD + 0.1,
              "n2", LOW_THRESHOLD + 0.1,
              "n3", (LOW_THRESHOLD + 0.1) *
                  OutlierDetector.MEDIAN_MULTIPLIER + 0.1),
              ImmutableSet.of("n3"))

          // Only the statistical outliers n3 and n11 should be returned.
          .put(new ImmutableMap.Builder<String, Double>()
                  .put("n1", 1029.4322)
                  .put("n2", 2647.876)
                  .put("n3", 9194.312)
                  .put("n4", 2.2)
                  .put("n5", 2012.92)
                  .put("n6", 1843.81)
                  .put("n7", 1201.43)
                  .put("n8", 6712.01)
                  .put("n9", 3278.554)
                  .put("n10", 2091.765)
                  .put("n11", 9194.77).build(),
              ImmutableSet.of("n3", "n11"))

          // The following input set has multiple outliers.
          //   - The low outliers (n4, n6) should not be returned.
          //   - High outlier n2 is within 3 multiples of the median
          //     and so it should not be returned.
          //   - Only the high outlier n8 should be returned.
          .put(new ImmutableMap.Builder<String, Double>()
                  .put("n1", 5002.0)
                  .put("n2", 9001.0)
                  .put("n3", 5004.0)
                  .put("n4", 1001.0)
                  .put("n5", 5003.0)
                  .put("n6", 2001.0)
                  .put("n7", 5000.0)
                  .put("n8", 101002.0)
                  .put("n9", 5001.0)
                  .put("n10", 5002.0)
                  .put("n11", 5105.0)
                  .put("n12", 5006.0).build(),
              ImmutableSet.of("n8"))

          .build();


  private OutlierDetector slowNodeDetector;

  @Before
  public void setup() {
    slowNodeDetector = new OutlierDetector(MIN_OUTLIER_DETECTION_PEERS,
        (long) LOW_THRESHOLD);
    GenericTestUtils.setLogLevel(OutlierDetector.LOG, Level.ALL);
  }

  @Test
  public void testOutliersFromTestMatrix() {
    for (Map.Entry<Map<String, Double>, Set<String>> entry :
        outlierTestMatrix.entrySet()) {

      LOG.info("Verifying set {}", entry.getKey());
      final Set<String> outliers =
          slowNodeDetector.getOutliers(entry.getKey()).keySet();
      assertTrue(
          "Running outlier detection on " + entry.getKey() +
              " was expected to yield set " + entry.getValue() + ", but " +
              " we got set " + outliers,
          outliers.equals(entry.getValue()));
    }
  }

  /**
   * Unit test for {@link OutlierDetector#computeMedian(List)}.
   */
  @Test
  public void testMediansFromTestMatrix() {
    for (Map.Entry<List<Double>, Pair<Double, Double>> entry :
        medianTestMatrix.entrySet()) {
      final List<Double> inputList = new ArrayList<>(entry.getKey());
      Collections.sort(inputList);
      final Double median = OutlierDetector.computeMedian(inputList);
      final Double expectedMedian = entry.getValue().getLeft();

      // Ensure that the median is within 0.001% of expected.
      // We need some fudge factor for floating point comparison.
      final Double errorPercent =
          Math.abs(median - expectedMedian) * 100.0 / expectedMedian;

      assertTrue(
          "Set " + inputList + "; Expected median: " +
              expectedMedian + ", got: " + median,
          errorPercent < 0.001);
    }
  }

  /**
   * Unit test for {@link OutlierDetector#computeMad(List)}.
   */
  @Test
  public void testMadsFromTestMatrix() {
    for (Map.Entry<List<Double>, Pair<Double, Double>> entry :
        medianTestMatrix.entrySet()) {
      final List<Double> inputList = new ArrayList<>(entry.getKey());
      Collections.sort(inputList);
      final Double mad = OutlierDetector.computeMad(inputList);
      final Double expectedMad = entry.getValue().getRight();

      // Ensure that the MAD is within 0.001% of expected.
      // We need some fudge factor for floating point comparison.
      if (entry.getKey().size() > 1) {
        final Double errorPercent =
            Math.abs(mad - expectedMad) * 100.0 / expectedMad;

        assertTrue(
            "Set " + entry.getKey() + "; Expected M.A.D.: " +
                expectedMad + ", got: " + mad,
            errorPercent < 0.001);
      } else {
        // For an input list of size 1, the MAD should be 0.0.
        final Double epsilon = 0.000001; // Allow for some FP math error.
        assertTrue(
            "Set " + entry.getKey() + "; Expected M.A.D.: " +
                expectedMad + ", got: " + mad,
            mad < epsilon);
      }
    }
  }

  /**
   * Verify that {@link OutlierDetector#computeMedian(List)} throws when
   * passed an empty list.
   */
  @Test(expected=IllegalArgumentException.class)
  public void testMedianOfEmptyList() {
    OutlierDetector.computeMedian(Collections.emptyList());
  }

  /**
   * Verify that {@link OutlierDetector#computeMad(List)} throws when
   * passed an empty list.
   */
  @Test(expected=IllegalArgumentException.class)
  public void testMadOfEmptyList() {
    OutlierDetector.computeMedian(Collections.emptyList());
  }
}
