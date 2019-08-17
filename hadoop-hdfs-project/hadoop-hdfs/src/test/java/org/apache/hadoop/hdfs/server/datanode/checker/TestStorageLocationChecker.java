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

package org.apache.hadoop.hdfs.server.datanode.checker;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.util.FakeTimer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DISK_CHECK_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY;
import static org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the {@link StorageLocationChecker} class.
 */
public class TestStorageLocationChecker {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestStorageLocationChecker.class);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  /**
   * Verify that all healthy locations are correctly handled and that the
   * check routine is invoked as expected.
   * @throws Exception
   */
  @Test(timeout=30000)
  public void testAllLocationsHealthy() throws Exception {
    final List<StorageLocation> locations =
        makeMockLocations(HEALTHY, HEALTHY, HEALTHY);
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, 0);
    StorageLocationChecker checker =
        new StorageLocationChecker(conf, new FakeTimer());
    List<StorageLocation> filteredLocations = checker.check(conf, locations);

    // All locations should be healthy.
    assertThat(filteredLocations.size(), is(3));

    // Ensure that the check method was invoked for each location.
    for (StorageLocation location : locations) {
      verify(location).check(any(StorageLocation.CheckContext.class));
    }
  }

  /**
   * Test handling when the number of failed locations is below the
   * max volume failure threshold.
   *
   * @throws Exception
   */
  @Test(timeout=30000)
  public void testFailedLocationsBelowThreshold() throws Exception {
    final List<StorageLocation> locations =
        makeMockLocations(HEALTHY, HEALTHY, FAILED); // 2 healthy, 1 failed.
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, 1);
    StorageLocationChecker checker =
        new StorageLocationChecker(conf, new FakeTimer());
    List<StorageLocation> filteredLocations = checker.check(conf, locations);
    assertThat(filteredLocations.size(), is(2));
  }

  /**
   * Test handling when the number of volume failures tolerated is the
   * same as the number of volumes.
   *
   * @throws Exception
   */
  @Test(timeout=30000)
  public void testFailedLocationsAboveThreshold() throws Exception {
    final List<StorageLocation> locations =
        makeMockLocations(HEALTHY, FAILED, FAILED); // 1 healthy, 2 failed.
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, 1);

    thrown.expect(IOException.class);
    thrown.expectMessage("Too many failed volumes - current valid volumes: 1,"
        + " volumes configured: 3, volumes failed: 2, volume failures"
        + " tolerated: 1");
    StorageLocationChecker checker =
        new StorageLocationChecker(conf, new FakeTimer());
    checker.check(conf, locations);
  }

  /**
   * Test handling all storage locations are failed.
   *
   * @throws Exception
   */
  @Test(timeout=30000)
  public void testBadConfiguration() throws Exception {
    final List<StorageLocation> locations =
        makeMockLocations(HEALTHY, HEALTHY, HEALTHY);
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, 3);

    thrown.expect(HadoopIllegalArgumentException.class);
    thrown.expectMessage("Invalid value configured");
    StorageLocationChecker checker =
        new StorageLocationChecker(conf, new FakeTimer());
    checker.check(conf, locations);
  }

  /**
   * Verify that a {@link StorageLocation#check} timeout is correctly detected
   * as a failure.
   *
   * This is hard to test without a {@link Thread#sleep} call.
   *
   * @throws Exception
   */
  @Test (timeout=300000)
  public void testTimeoutInCheck() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setTimeDuration(DFS_DATANODE_DISK_CHECK_TIMEOUT_KEY,
        1, TimeUnit.SECONDS);
    conf.setInt(DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, 1);
    final FakeTimer timer = new FakeTimer();

    // Generate a list of storage locations the first of which sleeps
    // for 2 seconds in its check() routine.
    final List<StorageLocation> locations = makeSlowLocations(2000, 1);
    StorageLocationChecker checker =
        new StorageLocationChecker(conf, timer);

    try {
      // Check the two locations and ensure that only one of them
      // was filtered out.
      List<StorageLocation> filteredList = checker.check(conf, locations);
      assertThat(filteredList.size(), is(1));
    } finally {
      checker.shutdownAndWait(10, TimeUnit.SECONDS);
    }
  }

  /**
   * Return a list of storage locations - one per argument - which return
   * health check results corresponding to the supplied arguments.
   */
  private List<StorageLocation> makeMockLocations(VolumeCheckResult... args)
      throws IOException {
    final List<StorageLocation> locations = new ArrayList<>(args.length);
    final AtomicInteger index = new AtomicInteger(0);

    for (VolumeCheckResult result : args) {
      final StorageLocation location = mock(StorageLocation.class);
      when(location.toString()).thenReturn("/" + index.incrementAndGet());
      when(location.check(any(StorageLocation.CheckContext.class)))
          .thenReturn(result);
      locations.add(location);
    }

    return locations;
  }

  /**
   * Return a list of storage locations - one per argument - whose check()
   * method takes at least the specified number of milliseconds to complete.
   */
  private List<StorageLocation> makeSlowLocations(long... args)
      throws IOException {
    final List<StorageLocation> locations = new ArrayList<>(args.length);
    final AtomicInteger index = new AtomicInteger(0);

    for (final long checkDelayMs: args) {
      final StorageLocation location = mock(StorageLocation.class);
      when(location.toString()).thenReturn("/" + index.incrementAndGet());
      when(location.check(any(StorageLocation.CheckContext.class)))
          .thenAnswer(new Answer<VolumeCheckResult>() {
            @Override
            public VolumeCheckResult answer(InvocationOnMock invocation)
                throws Throwable {
              Thread.sleep(checkDelayMs);
              return VolumeCheckResult.HEALTHY;
            }
          });

      locations.add(location);
    }
    return locations;
  }

  @Test
  public void testInvalidConfigurationValues() throws Exception {
    final List<StorageLocation> locations =
        makeMockLocations(HEALTHY, HEALTHY, HEALTHY);
    Configuration conf = new HdfsConfiguration();

    conf.setInt(DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, 4);
    intercept(HadoopIllegalArgumentException.class,
        "Invalid value configured for dfs.datanode.failed.volumes.tolerated"
            + " - 4. Value configured is >= to the "
            + "number of configured volumes (3).",
        () -> new StorageLocationChecker(conf, new FakeTimer()).check(conf,
            locations));
    conf.unset(DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY);

    conf.setInt(DFS_DATANODE_DISK_CHECK_TIMEOUT_KEY, 0);
    intercept(HadoopIllegalArgumentException.class,
        "Invalid value configured for dfs.datanode.disk.check.timeout"
            + " - 0 (should be > 0)",
        () -> new StorageLocationChecker(conf, new FakeTimer()));
    conf.unset(DFS_DATANODE_DISK_CHECK_TIMEOUT_KEY);

    conf.setInt(DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, -2);
    intercept(HadoopIllegalArgumentException.class,
        "Invalid value configured for dfs.datanode.failed.volumes.tolerated"
            + " - -2 should be greater than or equal to -1",
        () -> new StorageLocationChecker(conf, new FakeTimer()));
  }
}
