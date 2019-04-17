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

package org.apache.hadoop.ozone.container.common.volume;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.Timer;

import com.google.common.collect.Iterables;
import org.apache.commons.io.FileUtils;
import org.apache.curator.shaded.com.google.common.collect.ImmutableSet;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.hamcrest.CoreMatchers.is;
import org.junit.After;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Verify that {@link VolumeSet} correctly checks for failed disks
 * during initialization.
 */
public class TestVolumeSetDiskChecks {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestVolumeSetDiskChecks.class);

  @Rule
  public Timeout globalTimeout = new Timeout(30_000);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private Configuration conf = null;

  /**
   * Cleanup volume directories.
   */
  @After
  public void cleanup() {
    final Collection<String> dirs = conf.getTrimmedStringCollection(
        DFS_DATANODE_DATA_DIR_KEY);

    for (String d: dirs) {
      FileUtils.deleteQuietly(new File(d));
    }
  }

  /**
   * Verify that VolumeSet creates volume root directories at startup.
   * @throws IOException
   */
  @Test
  public void testOzoneDirsAreCreated() throws IOException {
    final int numVolumes = 2;

    conf = getConfWithDataNodeDirs(numVolumes);
    final VolumeSet volumeSet =
        new VolumeSet(UUID.randomUUID().toString(), conf);

    assertThat(volumeSet.getVolumesList().size(), is(numVolumes));
    assertThat(volumeSet.getFailedVolumesList().size(), is(0));

    // Verify that the Ozone dirs were created during initialization.
    Collection<String> dirs = conf.getTrimmedStringCollection(
        DFS_DATANODE_DATA_DIR_KEY);
    for (String d : dirs) {
      assertTrue(new File(d).isDirectory());
    }
  }

  /**
   * Verify that bad volumes are filtered at startup.
   * @throws IOException
   */
  @Test
  public void testBadDirectoryDetection() throws IOException {
    final int numVolumes = 5;
    final int numBadVolumes = 2;

    conf = getConfWithDataNodeDirs(numVolumes);
    final VolumeSet volumeSet = new VolumeSet(
        UUID.randomUUID().toString(), conf) {
      @Override
      HddsVolumeChecker getVolumeChecker(Configuration configuration)
          throws DiskErrorException {
        return new DummyChecker(configuration, new Timer(), numBadVolumes);
      }
    };

    assertThat(volumeSet.getFailedVolumesList().size(), is(numBadVolumes));
    assertThat(volumeSet.getVolumesList().size(),
        is(numVolumes - numBadVolumes));
  }

  /**
   * Verify that initialization fails if all volumes are bad.
   */
  @Test
  public void testAllVolumesAreBad() throws IOException {
    final int numVolumes = 5;

    conf = getConfWithDataNodeDirs(numVolumes);
    thrown.expect(IOException.class);
    final VolumeSet volumeSet = new VolumeSet(
        UUID.randomUUID().toString(), conf) {
      @Override
      HddsVolumeChecker getVolumeChecker(Configuration configuration)
          throws DiskErrorException {
        return new DummyChecker(configuration, new Timer(), numVolumes);
      }
    };
  }

  /**
   * Update configuration with the specified number of Datanode
   * storage directories.
   * @param conf
   * @param numDirs
   */
  private Configuration getConfWithDataNodeDirs(int numDirs) {
    final Configuration ozoneConf = new OzoneConfiguration();
    final List<String> dirs = new ArrayList<>();
    for (int i = 0; i < numDirs; ++i) {
      dirs.add(GenericTestUtils.getRandomizedTestDir().getPath());
    }
    ozoneConf.set(DFS_DATANODE_DATA_DIR_KEY, String.join(",", dirs));
    return ozoneConf;
  }

  /**
   * A no-op checker that fails the given number of volumes and succeeds
   * the rest.
   */
  static class DummyChecker extends HddsVolumeChecker {
    private final int numBadVolumes;

    DummyChecker(Configuration conf, Timer timer, int numBadVolumes)
        throws DiskErrorException {
      super(conf, timer);
      this.numBadVolumes = numBadVolumes;
    }

    @Override
    public Set<HddsVolume> checkAllVolumes(Collection<HddsVolume> volumes)
        throws InterruptedException {
      // Return the first 'numBadVolumes' as failed.
      return ImmutableSet.copyOf(Iterables.limit(volumes, numBadVolumes));
    }
  }
}
