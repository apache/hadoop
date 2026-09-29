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

package org.apache.hadoop.io.wrappedio.impl;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatisticsContext;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.apache.hadoop.util.functional.Tuples;

import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticCounter;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfInvocation;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for IOStatistics wrapping.
 * <p>
 * This mixes direct use of the API to generate statistics data for
 * the reflection accessors to retrieve and manipulate.
 */
public class TestWrappedStatistics extends AbstractHadoopTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestWrappedIO.class);

  /**
   * Stub Serializable.
   */
  private static final Serializable SERIALIZABLE = new Serializable() {};

  /**
   * Dynamically Wrapped IO statistics.
   */
  private final DynamicWrappedStatistics statistics = new DynamicWrappedStatistics();

  /**
   * Local FS.
   */
  private LocalFileSystem local;

  /**
   * Path to temporary file.
   */
  private Path jsonPath;

  @BeforeEach
  public void setUp() throws Exception {
    String testDataDir = new FileSystemTestHelper().getTestRootDir();
    File tempDir = new File(testDataDir);
    local = FileSystem.getLocal(new Configuration());
    // Temporary file.
    File jsonFile = new File(tempDir, "snapshot.json");
    jsonPath = new Path(jsonFile.toURI());
  }

  /**
   * The class must load, with all method groups available.
   */
  @Test
  public void testLoaded() throws Throwable {
    assertThat(statistics.ioStatisticsAvailable())
        .describedAs("IOStatistics class must be available")
        .isTrue();
    assertThat(statistics.ioStatisticsContextAvailable())
        .describedAs("IOStatisticsContext must be available")
        .isTrue();
  }

  @Test
  public void testCreateEmptySnapshot() throws Throwable {
    assertThat(statistics.iostatisticsSnapshot_create())
        .describedAs("iostatisticsSnapshot_create()")
        .isInstanceOf(IOStatisticsSnapshot.class)
        .satisfies(statistics::isIOStatisticsSnapshot)
        .satisfies(statistics::isIOStatistics);
  }

  @Test
  public void testCreateNullSource() throws Throwable {
    assertThat(statistics.iostatisticsSnapshot_create(null))
        .describedAs("iostatisticsSnapshot_create(null)")
        .isInstanceOf(IOStatisticsSnapshot.class);
  }

  @Test
  public void testCreateOther() throws Throwable {
    assertThat(statistics.iostatisticsSnapshot_create(null))
        .describedAs("iostatisticsSnapshot_create(null)")
        .isInstanceOf(IOStatisticsSnapshot.class);
  }

  @Test
  public void testCreateNonIOStatsSource() throws Throwable {
    intercept(ClassCastException.class, () ->
        statistics.iostatisticsSnapshot_create("hello"));
  }

  @Test
  public void testRetrieveNullSource() throws Throwable {
    assertThat(statistics.iostatisticsSnapshot_retrieve(null))
        .describedAs("iostatisticsSnapshot_retrieve(null)")
        .isNull();
  }

  @Test
  public void testRetrieveNonIOStatsSource() throws Throwable {
    assertThat(statistics.iostatisticsSnapshot_retrieve(this))
        .describedAs("iostatisticsSnapshot_retrieve(this)")
        .isNull();
  }

  /**
   * Assert handling of json serialization for null value.
   */
  @Test
  public void testNullInstanceToJson() throws Throwable {
    intercept(IllegalArgumentException.class, () -> toJsonString(null));
  }

  /**
   * Assert handling of json serialization for wrong value.
   */
  @Test
  public void testWrongSerializableTypeToJson() throws Throwable {
    intercept(IllegalArgumentException.class, () -> toJsonString(SERIALIZABLE));
  }

  /**
   * Try to aggregate into the wrong type.
   */
  @Test
  public void testAggregateWrongSerializable() throws Throwable {
    intercept(IllegalArgumentException.class, () ->
        statistics.iostatisticsSnapshot_aggregate(SERIALIZABLE,
            statistics.iostatisticsContext_getCurrent()));
  }

  /**
   * Try to save the wrong type.
   */
  @Test
  public void testSaveWrongSerializable() throws Throwable {
    intercept(IllegalArgumentException.class, () ->
        statistics.iostatisticsSnapshot_save(SERIALIZABLE, local, jsonPath, true));
  }

  /**
   * Test all the IOStatisticsContext operations, including
   * JSON round trip of the statistics.
   */
  @Test
  public void testIOStatisticsContextMethods() {

    assertThat(statistics.ioStatisticsContextAvailable())
        .describedAs("ioStatisticsContextAvailable() of %s", statistics)
        .isTrue();
    assertThat(statistics.iostatisticsContext_enabled())
        .describedAs("iostatisticsContext_enabled() of %s", statistics)
        .isTrue();

    // get the current context, validate it
    final Object current = statistics.iostatisticsContext_getCurrent();
    assertThat(current)
        .describedAs("IOStatisticsContext")
        .isInstanceOf(IOStatisticsContext.class)
        .satisfies(statistics::isIOStatisticsSource);

    // take a snapshot
    final Serializable snapshot = statistics.iostatisticsContext_snapshot();
    assertThat(snapshot)
        .satisfies(statistics::isIOStatisticsSnapshot);

    // use the retrieve API to create a snapshot from the IOStatisticsSource interface
    final Serializable retrieved = statistics.iostatisticsSnapshot_retrieve(current);
    assertJsonEqual(retrieved, snapshot);

    // to/from JSON
    final String json = toJsonString(snapshot);
    LOG.info("Serialized to json {}", json);
    final Serializable snap2 = statistics.iostatisticsSnapshot_fromJsonString(json);
    assertJsonEqual(snap2, snapshot);

    // get the values
    statistics.iostatistics_counters(snapshot);
    statistics.iostatistics_gauges(snapshot);
    statistics.iostatistics_minimums(snapshot);
    statistics.iostatistics_maximums(snapshot);
    statistics.iostatistics_means(snapshot);

    // set to null
    statistics.iostatisticsContext_setThreadIOStatisticsContext(null);

    assertThat(statistics.iostatisticsContext_getCurrent())
        .describedAs("current IOStatisticsContext after resetting")
        .isNotSameAs(current);

    // then set to the "current"  value
    statistics.iostatisticsContext_setThreadIOStatisticsContext(current);

    assertThat(statistics.iostatisticsContext_getCurrent())
        .describedAs("current IOStatisticsContext after resetting")
        .isSameAs(current);

    // and reset
    statistics.iostatisticsContext_reset();

    // now aggregate the retrieved stats into it.
    assertThat(statistics.iostatisticsContext_aggregate(retrieved))
        .describedAs("iostatisticsContext_aggregate of %s", retrieved)
        .isTrue();
  }


  /**
   * Perform some real IOStatisticsContext operations.
   */
  @Test
  public void testIOStatisticsContextInteraction() {
    statistics.iostatisticsContext_reset();

    // create a snapshot with a counter
    final IOStatisticsSnapshot snapshot =
        (IOStatisticsSnapshot) statistics.iostatisticsSnapshot_create();
    snapshot.setCounter("c1", 10);

    // aggregate twice
    statistics.iostatisticsContext_aggregate(snapshot);
    statistics.iostatisticsContext_aggregate(snapshot);

    // take a snapshot
    final IOStatisticsSnapshot snap2 =
        (IOStatisticsSnapshot) statistics.iostatisticsContext_snapshot();

    // assert the valuue
    assertThatStatisticCounter(snap2, "c1")
        .isEqualTo(20);
  }

  /**
   * Expect that two IOStatisticsInstances serialized to exactly the same JSON.
   * @param actual actual value.
   * @param expected expected value
   */
  private void assertJsonEqual(Serializable actual, Serializable expected) {
    assertThat(toJsonString(actual))
        .describedAs("JSON format string of %s", actual)
        .isEqualTo(toJsonString(expected));
  }

  /**
   * Convert a snapshot to a JSON string.
   * @param snapshot IOStatisticsSnapshot
   * @return a JSON serialization.
   */
  private String toJsonString(final Serializable snapshot) {
    return statistics.iostatisticsSnapshot_toJsonString(snapshot);
  }

  /**
   * Create an empty snapshot, save it then load back.
   */
  @Test
  public void testLocalSaveOfEmptySnapshot() throws Throwable {
    final Serializable snapshot = statistics.iostatisticsSnapshot_create();
    statistics.iostatisticsSnapshot_save(snapshot, local, jsonPath, true);
    final Serializable loaded = statistics.iostatisticsSnapshot_load(local, jsonPath);
    LOG.info("loaded statistics {}",
        statistics.iostatistics_toPrettyString(loaded));

    // now try to save over the same path with overwrite false
    intercept(UncheckedIOException.class, () ->
        statistics.iostatisticsSnapshot_save(snapshot, local, jsonPath, false));

    // after delete the load fails
    local.delete(jsonPath, false);
    intercept(UncheckedIOException.class, () ->
        statistics.iostatisticsSnapshot_load(local, jsonPath));
  }

  /**
   * Build up a complex statistic and assert extraction on it.
   */
  @Test
  public void testStatisticExtraction() throws Throwable {

    final IOStatisticsStore store = IOStatisticsBinding.iostatisticsStore()
        .withCounters("c1", "c2")
        .withGauges("g1")
        .withDurationTracking("d1", "d2")
        .build();

    store.incrementCounter("c1");
    store.setGauge("g1", 10);
    trackDurationOfInvocation(store, "d1", () ->
        sleep(20));
    store.trackDuration("d1").close();

    intercept(IOException.class, () ->
        trackDurationOfInvocation(store, "d2", () -> {
          sleep(10);
          throw new IOException("generated");
        }));

    final Serializable snapshot = statistics.iostatisticsSnapshot_create(store);


    // complex round trip
    statistics.iostatisticsSnapshot_save(snapshot, local, jsonPath, true);
    final Serializable loaded = statistics.iostatisticsSnapshot_load(local, jsonPath);
    LOG.info("loaded statistics {}",
        statistics.iostatistics_toPrettyString(loaded));
    assertJsonEqual(loaded, snapshot);


    // get the values
    assertThat(statistics.iostatistics_counters(loaded))
        .containsOnlyKeys("c1", "c2",
            "d1", "d1.failures",
            "d2", "d2.failures")
        .containsEntry("c1", 1L)
        .containsEntry("d1", 2L)
        .containsEntry("d2", 1L);
    assertThat(statistics.iostatistics_gauges(loaded))
        .containsOnlyKeys("g1")
        .containsEntry("g1", 10L);

    final Map<String, Long> minimums = statistics.iostatistics_minimums(snapshot);
    assertThat(minimums)
        .containsEntry("d1.min", 0L);
    final long d2FailuresMin = minimums.get("d2.failures.min");
    assertThat(d2FailuresMin)
        .describedAs("min d2.failures")
        .isGreaterThan(0);
    final Map<String, Long> maximums = statistics.iostatistics_maximums(snapshot);
    assertThat(maximums)
        .containsEntry("d2.failures.max", d2FailuresMin);
    final long d1Max = maximums.get("d1.max");


    final Map<String, Map.Entry<Long, Long>> means =
        statistics.iostatistics_means(snapshot);

    assertThat(means)
        .containsEntry("d1.mean", Tuples.pair(2L, d1Max))
        .containsEntry("d2.failures.mean", Tuples.pair(1L, d2FailuresMin));

  }

  /**
   * Sleep for some milliseconds; interruptions are swallowed.
   * @param millis time in milliseconds
   */
  private static void sleep(final int millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ignored) {

    }
  }

  /**
   * Bind to an empty class to simulate a runtime where none of the methods were found
   * through reflection, and verify the expected failure semantics.
   */
  @Test
  public void testMissingIOStatisticsMethods() throws Throwable {
    final DynamicWrappedStatistics missing =
        new DynamicWrappedStatistics(StubClass.class.getName());

    // probes which just return false
    assertThat(missing.ioStatisticsAvailable())
        .describedAs("ioStatisticsAvailable() of %s", missing)
        .isFalse();

    // probes of type of argument which return false if the
    // methods are missing
    assertThat(missing.isIOStatistics(SERIALIZABLE))
        .describedAs("isIOStatistics() of %s", missing)
        .isFalse();
    assertThat(missing.isIOStatisticsSource(SERIALIZABLE))
        .describedAs("isIOStatisticsSource() of %s", missing)
        .isFalse();
    assertThat(missing.isIOStatisticsSnapshot(SERIALIZABLE))
        .describedAs("isIOStatisticsSnapshot() of %s", missing)
        .isFalse();

    // operations which raise exceptions
    intercept(UnsupportedOperationException.class, () ->
        missing.iostatisticsSnapshot_create());

    intercept(UnsupportedOperationException.class, () ->
        missing.iostatisticsSnapshot_create(this));

    intercept(UnsupportedOperationException.class, () ->
        missing.iostatisticsSnapshot_aggregate(SERIALIZABLE, this));

    intercept(UnsupportedOperationException.class, () ->
        missing.iostatisticsSnapshot_fromJsonString("{}"));
    intercept(UnsupportedOperationException.class, () ->
        missing.iostatisticsSnapshot_toJsonString(SERIALIZABLE));

    final Path path = new Path("/");

    intercept(UnsupportedOperationException.class, () ->
        missing.iostatisticsSnapshot_load(local, path));

    intercept(UnsupportedOperationException.class, () ->
        missing.iostatisticsSnapshot_save(SERIALIZABLE, local, path, true));

    intercept(UnsupportedOperationException.class, () ->
        missing.iostatisticsSnapshot_retrieve(this));

    intercept(UnsupportedOperationException.class, () ->
        missing.iostatistics_toPrettyString(this));

  }


  /**
   * Empty class to bind against and ensure all methods fail to bind.
   */
  private static final class StubClass { }

  /**
   * Bind to {@link StubClass} to simulate a runtime where none of the methods were found
   * through reflection, and verify the expected failure semantics.
   */
  @Test
  public void testMissingContextMethods() throws Throwable {
    final DynamicWrappedStatistics missing =
        new DynamicWrappedStatistics(StubClass.class.getName());

    // probes which just return false
    assertThat(missing.ioStatisticsContextAvailable())
        .describedAs("ioStatisticsContextAvailable() of %s", missing)
        .isFalse();
    assertThat(missing.iostatisticsContext_enabled())
        .describedAs("iostatisticsContext_enabled() of %s", missing)
        .isFalse();

    // operations which raise exceptions
    intercept(UnsupportedOperationException.class, missing::iostatisticsContext_reset);
    intercept(UnsupportedOperationException.class, missing::iostatisticsContext_getCurrent);
    intercept(UnsupportedOperationException.class, missing::iostatisticsContext_snapshot);
    intercept(UnsupportedOperationException.class, () ->
        missing.iostatisticsContext_setThreadIOStatisticsContext(null));
  }


  /**
   * Validate class checks in {@code iostatisticsSnapshot_aggregate()}.
   */
  @Test
  public void testStatisticCasting() throws Throwable {
    Serializable iostats = statistics.iostatisticsSnapshot_create(null);
    final String wrongType = "wrong type";
    intercept(IllegalArgumentException.class, () ->
        statistics.iostatisticsSnapshot_aggregate(iostats, wrongType));
  }

}


