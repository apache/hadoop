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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.*;
import org.apache.hadoop.util.FakeTimer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.ClosedChannelException;
import java.util.concurrent.TimeUnit;
import java.util.*;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;


/**
 * Test a few more conditions not covered by TestDatasetVolumeChecker.
 */
public class TestDatasetVolumeCheckerFailures {
  public static final Logger LOG =LoggerFactory.getLogger(
      TestDatasetVolumeCheckerFailures.class);

  private FakeTimer timer;
  private Configuration conf;

  private static final long MIN_DISK_CHECK_GAP_MS = 1000; // 1 second.

  @Before
  public void commonInit() {
    timer = new FakeTimer();
    conf = new HdfsConfiguration();
    conf.setTimeDuration(DFSConfigKeys.DFS_DATANODE_DISK_CHECK_MIN_GAP_KEY,
        MIN_DISK_CHECK_GAP_MS, TimeUnit.MILLISECONDS);
  }

  /**
   * Test timeout in {@link DatasetVolumeChecker#checkAllVolumes}.
   * @throws Exception
   */
  @Test(timeout=60000)
  public void testTimeout() throws Exception {
    // Add a volume whose check routine hangs forever.
    final List<FsVolumeSpi> volumes =
        Collections.singletonList(makeHungVolume());

    final FsDatasetSpi<FsVolumeSpi> dataset =
        TestDatasetVolumeChecker.makeDataset(volumes);

    // Create a disk checker with a very low timeout.
    conf.setTimeDuration(DFSConfigKeys.DFS_DATANODE_DISK_CHECK_TIMEOUT_KEY,
        1, TimeUnit.SECONDS);
    final DatasetVolumeChecker checker =
        new DatasetVolumeChecker(conf, new FakeTimer());

    // Ensure that the hung volume is detected as failed.
    Set<FsVolumeSpi> failedVolumes = checker.checkAllVolumes(dataset);
    assertThat(failedVolumes.size(), is(1));
  }

  /**
   * Test checking a closed volume i.e. one which cannot be referenced.
   *
   * @throws Exception
   */
  @Test(timeout=60000)
  public void testCheckingClosedVolume() throws Exception {
    // Add a volume that cannot be referenced.
    final List<FsVolumeSpi> volumes =
        Collections.singletonList(makeClosedVolume());

    final FsDatasetSpi<FsVolumeSpi> dataset =
        TestDatasetVolumeChecker.makeDataset(volumes);

    DatasetVolumeChecker checker = new DatasetVolumeChecker(conf, timer);
    Set<FsVolumeSpi> failedVolumes = checker.checkAllVolumes(dataset);
    assertThat(failedVolumes.size(), is(0));
    assertThat(checker.getNumSyncDatasetChecks(), is(0L));

    // The closed volume should not have been checked as it cannot
    // be referenced.
    verify(volumes.get(0), times(0)).check(anyObject());
  }

  @Test(timeout=60000)
  public void testMinGapIsEnforcedForSyncChecks() throws Exception {
    final List<FsVolumeSpi> volumes =
        TestDatasetVolumeChecker.makeVolumes(1, VolumeCheckResult.HEALTHY);
    final FsDatasetSpi<FsVolumeSpi> dataset =
        TestDatasetVolumeChecker.makeDataset(volumes);
    final DatasetVolumeChecker checker = new DatasetVolumeChecker(conf, timer);

    checker.checkAllVolumes(dataset);
    assertThat(checker.getNumSyncDatasetChecks(), is(1L));

    // Re-check without advancing the timer. Ensure the check is skipped.
    checker.checkAllVolumes(dataset);
    assertThat(checker.getNumSyncDatasetChecks(), is(1L));
    assertThat(checker.getNumSkippedChecks(), is(1L));

    // Re-check after advancing the timer. Ensure the check is performed.
    timer.advance(MIN_DISK_CHECK_GAP_MS);
    checker.checkAllVolumes(dataset);
    assertThat(checker.getNumSyncDatasetChecks(), is(2L));
    assertThat(checker.getNumSkippedChecks(), is(1L));
  }

  /**
   * Create a mock FsVolumeSpi whose {@link FsVolumeSpi#check} routine
   * hangs forever.
   *
   * @return volume
   * @throws Exception
   */
  private static FsVolumeSpi makeHungVolume() throws Exception {
    final FsVolumeSpi volume = mock(FsVolumeSpi.class);
    final FsVolumeReference reference = mock(FsVolumeReference.class);
    final StorageLocation location = mock(StorageLocation.class);

    when(reference.getVolume()).thenReturn(volume);
    when(volume.obtainReference()).thenReturn(reference);
    when(volume.getStorageLocation()).thenReturn(location);
    when(volume.check(anyObject())).thenAnswer(
        new Answer<VolumeCheckResult>() {
        @Override
        public VolumeCheckResult answer(InvocationOnMock invocation)
            throws Throwable {
          Thread.sleep(Long.MAX_VALUE);     // Sleep forever.
          return VolumeCheckResult.HEALTHY; // unreachable.
        }
      });
    return volume;
  }

  /**
   * Create a mock FsVolumeSpi which is closed and hence cannot
   * be referenced.
   *
   * @return volume
   * @throws Exception
   */
  private static FsVolumeSpi makeClosedVolume() throws Exception {
    final FsVolumeSpi volume = mock(FsVolumeSpi.class);
    final StorageLocation location = mock(StorageLocation.class);

    when(volume.obtainReference()).thenThrow(new ClosedChannelException());
    when(volume.getStorageLocation()).thenReturn(location);
    return volume;
  }
}
