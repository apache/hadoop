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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.*;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi.VolumeCheckContext;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.FakeTimer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Tests for {@link DatasetVolumeChecker} when the {@link FsVolumeSpi#check}
 * method returns different values of {@link VolumeCheckResult}.
 */
@RunWith(Parameterized.class)
public class TestDatasetVolumeChecker {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestDatasetVolumeChecker.class);

  @Rule
  public TestName testName = new TestName();

  /**
   * Run each test case for each possible value of {@link VolumeCheckResult}.
   * Including "null" for 'throw exception'.
   * @return
   */
  @Parameters(name="{0}")
  public static Collection<Object[]> data() {
    List<Object[]> values = new ArrayList<>();
    for (VolumeCheckResult result : VolumeCheckResult.values()) {
      values.add(new Object[] {result});
    }
    values.add(new Object[] {null});
    return values;
  }

  /**
   * When null, the check call should throw an exception.
   */
  private final VolumeCheckResult expectedVolumeHealth;
  private static final int NUM_VOLUMES = 2;


  public TestDatasetVolumeChecker(VolumeCheckResult expectedVolumeHealth) {
    this.expectedVolumeHealth = expectedVolumeHealth;
  }

  /**
   * Test {@link DatasetVolumeChecker#checkVolume} propagates the
   * check to the delegate checker.
   *
   * @throws Exception
   */
  @Test(timeout = 10000)
  public void testCheckOneVolume() throws Exception {
    LOG.info("Executing {}", testName.getMethodName());
    final FsVolumeSpi volume = makeVolumes(1, expectedVolumeHealth).get(0);
    final DatasetVolumeChecker checker =
        new DatasetVolumeChecker(new HdfsConfiguration(), new FakeTimer());
    checker.setDelegateChecker(new DummyChecker());
    final AtomicLong numCallbackInvocations = new AtomicLong(0);

    /**
     * Request a check and ensure it triggered {@link FsVolumeSpi#check}.
     */
    boolean result =
        checker.checkVolume(volume, new DatasetVolumeChecker.Callback() {
          @Override
          public void call(Set<FsVolumeSpi> healthyVolumes,
                           Set<FsVolumeSpi> failedVolumes) {
            numCallbackInvocations.incrementAndGet();
            if (expectedVolumeHealth != null &&
                expectedVolumeHealth != FAILED) {
              assertThat(healthyVolumes.size(), is(1));
              assertThat(failedVolumes.size(), is(0));
            } else {
              assertThat(healthyVolumes.size(), is(0));
              assertThat(failedVolumes.size(), is(1));
            }
          }
        });

    GenericTestUtils.waitFor(() -> numCallbackInvocations.get() > 0, 5, 10000);

    // Ensure that the check was invoked at least once.
    verify(volume, times(1)).check(anyObject());
    if (result) {
      assertThat(numCallbackInvocations.get(), is(1L));
    }
  }

  /**
   * Test {@link DatasetVolumeChecker#checkAllVolumes} propagates
   * checks for all volumes to the delegate checker.
   *
   * @throws Exception
   */
  @Test(timeout = 10000)
  public void testCheckAllVolumes() throws Exception {
    LOG.info("Executing {}", testName.getMethodName());

    final List<FsVolumeSpi> volumes = makeVolumes(
        NUM_VOLUMES, expectedVolumeHealth);
    final FsDatasetSpi<FsVolumeSpi> dataset = makeDataset(volumes);
    final DatasetVolumeChecker checker =
        new DatasetVolumeChecker(new HdfsConfiguration(), new FakeTimer());
    checker.setDelegateChecker(new DummyChecker());

    Set<FsVolumeSpi> failedVolumes = checker.checkAllVolumes(dataset);
    LOG.info("Got back {} failed volumes", failedVolumes.size());

    if (expectedVolumeHealth == null || expectedVolumeHealth == FAILED) {
      assertThat(failedVolumes.size(), is(NUM_VOLUMES));
    } else {
      assertTrue(failedVolumes.isEmpty());
    }

    // Ensure each volume's check() method was called exactly once.
    for (FsVolumeSpi volume : volumes) {
      verify(volume, times(1)).check(anyObject());
    }
  }

  /**
   * A checker to wraps the result of {@link FsVolumeSpi#check} in
   * an ImmediateFuture.
   */
  static class DummyChecker
      implements AsyncChecker<VolumeCheckContext, VolumeCheckResult> {

    @Override
    public Optional<ListenableFuture<VolumeCheckResult>> schedule(
        Checkable<VolumeCheckContext, VolumeCheckResult> target,
        VolumeCheckContext context) {
      try {
        return Optional.of(
            Futures.immediateFuture(target.check(context)));
      } catch (Exception e) {
        LOG.info("check routine threw exception " + e);
        return Optional.of(Futures.immediateFailedFuture(e));
      }
    }

    @Override
    public void shutdownAndWait(long timeout, TimeUnit timeUnit)
        throws InterruptedException {
      // Nothing to cancel.
    }
  }

  /**
   * Create a dataset with the given volumes.
   */
  static FsDatasetSpi<FsVolumeSpi> makeDataset(List<FsVolumeSpi> volumes)
      throws Exception {
    // Create dataset and init volume health.
    final FsDatasetSpi<FsVolumeSpi> dataset = mock(FsDatasetSpi.class);
    final FsDatasetSpi.FsVolumeReferences references = new
        FsDatasetSpi.FsVolumeReferences(volumes);
    when(dataset.getFsVolumeReferences()).thenReturn(references);
    return dataset;
  }

  static List<FsVolumeSpi> makeVolumes(
      int numVolumes, VolumeCheckResult health) throws Exception {
    final List<FsVolumeSpi> volumes = new ArrayList<>(numVolumes);
    for (int i = 0; i < numVolumes; ++i) {
      final FsVolumeSpi volume = mock(FsVolumeSpi.class);
      final FsVolumeReference reference = mock(FsVolumeReference.class);
      final StorageLocation location = mock(StorageLocation.class);

      when(reference.getVolume()).thenReturn(volume);
      when(volume.obtainReference()).thenReturn(reference);
      when(volume.getStorageLocation()).thenReturn(location);

      if (health != null) {
        when(volume.check(anyObject())).thenReturn(health);
      } else {
        final DiskErrorException de = new DiskErrorException("Fake Exception");
        when(volume.check(anyObject())).thenThrow(de);
      }
      volumes.add(volume);
    }
    return volumes;
  }
}