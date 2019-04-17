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

package org.apache.hadoop.ozone.container.keyvalue;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.OPEN;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.UNHEALTHY;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;

/**
 * Tests unhealthy container functionality in the {@link KeyValueContainer}
 * class.
 */
public class TestKeyValueContainerMarkUnhealthy {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestKeyValueContainerMarkUnhealthy.class);

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Rule
  public Timeout timeout = new Timeout(600_000);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private OzoneConfiguration conf;
  private String scmId = UUID.randomUUID().toString();
  private VolumeSet volumeSet;
  private RoundRobinVolumeChoosingPolicy volumeChoosingPolicy;
  private KeyValueContainerData keyValueContainerData;
  private KeyValueContainer keyValueContainer;
  private UUID datanodeId;

  @Before
  public void setUp() throws Exception {
    conf = new OzoneConfiguration();
    datanodeId = UUID.randomUUID();
    HddsVolume hddsVolume = new HddsVolume.Builder(folder.getRoot()
        .getAbsolutePath()).conf(conf).datanodeUuid(datanodeId
        .toString()).build();

    volumeSet = mock(VolumeSet.class);
    volumeChoosingPolicy = mock(RoundRobinVolumeChoosingPolicy.class);
    Mockito.when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
        .thenReturn(hddsVolume);

    keyValueContainerData = new KeyValueContainerData(1L,
        (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
        datanodeId.toString());
    final File metaDir = GenericTestUtils.getRandomizedTestDir();
    metaDir.mkdirs();
    keyValueContainerData.setMetadataPath(metaDir.getPath());


    keyValueContainer = new KeyValueContainer(
        keyValueContainerData, conf);
  }

  @After
  public void teardown() {
    volumeSet = null;
    keyValueContainer = null;
    keyValueContainerData = null;
  }

  /**
   * Verify that the .container file is correctly updated when a
   * container is marked as unhealthy.
   *
   * @throws IOException
   */
  @Test
  public void testMarkContainerUnhealthy() throws IOException {
    assertThat(keyValueContainerData.getState(), is(OPEN));
    keyValueContainer.markContainerUnhealthy();
    assertThat(keyValueContainerData.getState(), is(UNHEALTHY));

    // Check metadata in the .container file
    File containerFile = keyValueContainer.getContainerFile();

    keyValueContainerData = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
    assertThat(keyValueContainerData.getState(), is(UNHEALTHY));
  }

  /**
   * Attempting to close an unhealthy container should fail.
   * @throws IOException
   */
  @Test
  public void testCloseUnhealthyContainer() throws IOException {
    keyValueContainer.markContainerUnhealthy();
    thrown.expect(StorageContainerException.class);
    keyValueContainer.markContainerForClose();
  }

  /**
   * Attempting to mark a closed container as unhealthy should succeed.
   */
  @Test
  public void testMarkClosedContainerAsUnhealthy() throws IOException {
    // We need to create the container so the compact-on-close operation
    // does not NPE.
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
    keyValueContainer.close();
    keyValueContainer.markContainerUnhealthy();
    assertThat(keyValueContainerData.getState(), is(UNHEALTHY));
  }

  /**
   * Attempting to mark a quasi-closed container as unhealthy should succeed.
   */
  @Test
  public void testMarkQuasiClosedContainerAsUnhealthy() throws IOException {
    keyValueContainer.quasiClose();
    keyValueContainer.markContainerUnhealthy();
    assertThat(keyValueContainerData.getState(), is(UNHEALTHY));
  }

  /**
   * Attempting to mark a closing container as unhealthy should succeed.
   */
  @Test
  public void testMarkClosingContainerAsUnhealthy() throws IOException {
    keyValueContainer.markContainerForClose();
    keyValueContainer.markContainerUnhealthy();
    assertThat(keyValueContainerData.getState(), is(UNHEALTHY));
  }
}
