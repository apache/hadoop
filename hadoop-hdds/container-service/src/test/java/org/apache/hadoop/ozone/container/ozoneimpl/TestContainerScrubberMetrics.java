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
package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This test verifies the container scrubber metrics functionality.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestContainerScrubberMetrics {

  private final AtomicLong containerIdSeq = new AtomicLong(100);

  @Mock
  private Container<ContainerData> healthy;

  @Mock
  private Container<ContainerData> corruptMetadata;

  @Mock
  private Container<ContainerData> corruptData;

  @Mock
  private HddsVolume vol;

  private ContainerScrubberConfiguration conf;
  private ContainerController controller;

  @Before
  public void setup() {
    conf = new OzoneConfiguration()
        .getObject(ContainerScrubberConfiguration.class);
    conf.setMetadataScanInterval(0);
    conf.setDataScanInterval(0);
    controller = mockContainerController();
  }

  @Test
  public void testContainerMetaDataScrubberMetrics() {
    ContainerMetadataScanner subject =
        new ContainerMetadataScanner(conf, controller);
    subject.runIteration();

    ContainerMetadataScrubberMetrics metrics = subject.getMetrics();
    assertEquals(1, metrics.getNumScanIterations());
    assertEquals(3, metrics.getNumContainersScanned());
    assertEquals(1, metrics.getNumUnHealthyContainers());
  }

  @Test
  public void testContainerDataScrubberMetrics() {
    ContainerDataScanner subject =
        new ContainerDataScanner(conf, controller, vol);
    subject.runIteration();

    ContainerDataScrubberMetrics metrics = subject.getMetrics();
    assertEquals(1, metrics.getNumScanIterations());
    assertEquals(2, metrics.getNumContainersScanned());
    assertEquals(1, metrics.getNumUnHealthyContainers());
  }

  private ContainerController mockContainerController() {
    // healthy container
    setupMockContainer(healthy, true, true, true);

    // unhealthy container (corrupt data)
    setupMockContainer(corruptData, true, true, false);

    // unhealthy container (corrupt metadata)
    setupMockContainer(corruptMetadata, false, false, false);

    Collection<Container<?>> containers = Arrays.asList(
        healthy, corruptData, corruptMetadata);
    ContainerController mock = mock(ContainerController.class);
    when(mock.getContainers(vol)).thenReturn(containers.iterator());
    when(mock.getContainers()).thenReturn(containers.iterator());

    return mock;
  }

  private void setupMockContainer(
      Container<ContainerData> c, boolean shouldScanData,
      boolean scanMetaDataSuccess, boolean scanDataSuccess) {
    ContainerData data = mock(ContainerData.class);
    when(data.getContainerID()).thenReturn(containerIdSeq.getAndIncrement());
    when(c.getContainerData()).thenReturn(data);
    when(c.shouldScanData()).thenReturn(shouldScanData);
    when(c.scanMetaData()).thenReturn(scanMetaDataSuccess);
    when(c.scanData(any(DataTransferThrottler.class), any(Canceler.class)))
        .thenReturn(scanDataSuccess);
  }

}
