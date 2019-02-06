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
package org.apache.hadoop.hdfs.server.datanode.checker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.util.FakeTimer;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import org.mockito.stubbing.Answer;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Test that timeout is triggered during Disk Volume Checker.
 */
public class TestDatasetVolumeCheckerTimeout {
  public static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(TestDatasetVolumeCheckerTimeout.class);

  @Rule
  public TestName testName = new TestName();

  static Configuration conf;
  private static final long DISK_CHECK_TIMEOUT = 10;
  private static final long DISK_CHECK_TIME = 100;
  static ReentrantLock lock = new ReentrantLock();

  static {
    conf = new HdfsConfiguration();
    conf.setTimeDuration(
        DFSConfigKeys.DFS_DATANODE_DISK_CHECK_TIMEOUT_KEY,
        DISK_CHECK_TIMEOUT, TimeUnit.MILLISECONDS);
  }

  static FsVolumeSpi makeSlowVolume() throws Exception {
    final FsVolumeSpi volume = mock(FsVolumeSpi.class);
    final FsVolumeReference reference = mock(FsVolumeReference.class);
    final StorageLocation location = mock(StorageLocation.class);

    when(reference.getVolume()).thenReturn(volume);
    when(volume.obtainReference()).thenReturn(reference);
    when(volume.getStorageLocation()).thenReturn(location);

    when(volume.check(any())).thenAnswer(
        (Answer<VolumeCheckResult>) invocationOnMock -> {
        // Wait for the disk check to timeout and then release lock.
        lock.lock();
        lock.unlock();
        return VolumeCheckResult.HEALTHY;
      });

    return volume;
  }

  @Test (timeout = 300000)
  public void testDiskCheckTimeout() throws Exception {
    LOG.info("Executing {}", testName.getMethodName());
    final FsVolumeSpi volume = makeSlowVolume();

    final DatasetVolumeChecker checker =
        new DatasetVolumeChecker(conf, new FakeTimer());
    final AtomicLong numCallbackInvocations = new AtomicLong(0);

    lock.lock();
    /**
     * Request a check and ensure it triggered {@link FsVolumeSpi#check}.
     */
    boolean result =
        checker.checkVolume(volume, new DatasetVolumeChecker.Callback() {
          @Override
          public void call(Set<FsVolumeSpi> healthyVolumes,
              Set<FsVolumeSpi> failedVolumes) {
            numCallbackInvocations.incrementAndGet();

            // Assert that the disk check registers a failed volume due to
            // timeout
            assertThat(healthyVolumes.size(), is(0));
            assertThat(failedVolumes.size(), is(1));
          }
        });

    // Wait for the callback
    Thread.sleep(DISK_CHECK_TIME);

    // Release lock
    lock.unlock();

    // Ensure that the check was invoked only once.
    verify(volume, times(1)).check(any());
    assertThat(numCallbackInvocations.get(), is(1L));
  }
}
