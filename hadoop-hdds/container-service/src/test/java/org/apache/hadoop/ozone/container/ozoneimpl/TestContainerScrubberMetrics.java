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
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collection;

/**
 * This test verifies the container scrubber metrics functionality.
 */
public class TestContainerScrubberMetrics {
  @Test
  public void testContainerMetaDataScrubberMetrics() {
    OzoneConfiguration conf = new OzoneConfiguration();
    ContainerScrubberConfiguration c = conf.getObject(
        ContainerScrubberConfiguration.class);
    c.setMetadataScanInterval(0);
    HddsVolume vol = Mockito.mock(HddsVolume.class);
    ContainerController cntrl = mockContainerController(vol);

    ContainerMetadataScanner mc = new ContainerMetadataScanner(c, cntrl);
    mc.runIteration();

    Assert.assertEquals(1, mc.getMetrics().getNumScanIterations());
    Assert.assertEquals(3, mc.getMetrics().getNumContainersScanned());
    Assert.assertEquals(1, mc.getMetrics().getNumUnHealthyContainers());
  }

  @Test
  public void testContainerDataScrubberMetrics() {
    OzoneConfiguration conf = new OzoneConfiguration();
    ContainerScrubberConfiguration c = conf.getObject(
        ContainerScrubberConfiguration.class);
    c.setDataScanInterval(0);
    HddsVolume vol = Mockito.mock(HddsVolume.class);
    ContainerController cntrl = mockContainerController(vol);

    ContainerDataScanner sc = new ContainerDataScanner(c, cntrl, vol);
    sc.runIteration();

    ContainerDataScrubberMetrics m = sc.getMetrics();
    Assert.assertEquals(1, m.getNumScanIterations());
    Assert.assertEquals(2, m.getNumContainersScanned());
    Assert.assertEquals(1, m.getNumUnHealthyContainers());
  }

  private ContainerController mockContainerController(HddsVolume vol) {
    // healthy container
    Container<ContainerData> c1 = Mockito.mock(Container.class);
    Mockito.when(c1.shouldScanData()).thenReturn(true);
    Mockito.when(c1.scanMetaData()).thenReturn(true);
    Mockito.when(c1.scanData(
        Mockito.any(DataTransferThrottler.class),
        Mockito.any(Canceler.class))).thenReturn(true);

    // unhealthy container (corrupt data)
    ContainerData c2d = Mockito.mock(ContainerData.class);
    Mockito.when(c2d.getContainerID()).thenReturn(101L);
    Container<ContainerData> c2 = Mockito.mock(Container.class);
    Mockito.when(c2.scanMetaData()).thenReturn(true);
    Mockito.when(c2.shouldScanData()).thenReturn(true);
    Mockito.when(c2.scanData(
        Mockito.any(DataTransferThrottler.class),
        Mockito.any(Canceler.class))).thenReturn(false);
    Mockito.when(c2.getContainerData()).thenReturn(c2d);

    // unhealthy container (corrupt metadata)
    ContainerData c3d = Mockito.mock(ContainerData.class);
    Mockito.when(c3d.getContainerID()).thenReturn(102L);
    Container<ContainerData> c3 = Mockito.mock(Container.class);
    Mockito.when(c3.shouldScanData()).thenReturn(false);
    Mockito.when(c3.scanMetaData()).thenReturn(false);
    Mockito.when(c3.getContainerData()).thenReturn(c3d);

    Collection<Container<?>> containers = Arrays.asList(c1, c2, c3);
    ContainerController cntrl = Mockito.mock(ContainerController.class);
    Mockito.when(cntrl.getContainers(vol))
        .thenReturn(containers.iterator());
    Mockito.when(cntrl.getContainers())
        .thenReturn(containers.iterator());

    return cntrl;
  }

}
