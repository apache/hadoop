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
package org.apache.hadoop.yarn.server.resourcemanager.volume.csi;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.event.ControllerPublishVolumeEvent;
import org.apache.hadoop.yarn.server.volume.csi.CsiAdaptorClientProtocol;
import org.apache.hadoop.yarn.server.volume.csi.exception.InvalidVolumeException;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.event.ValidateVolumeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.VolumeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.VolumeState;
import org.apache.hadoop.yarn.server.volume.csi.exception.VolumeException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.*;

/**
 * Test cases for volume lifecycle management.
 */
public class TestVolumeLifecycle {

  @Test
  public void testValidation() throws InvalidVolumeException {
    VolumeImpl volume = (VolumeImpl) VolumeBuilder.newBuilder()
        .volumeId("test_vol_00000001")
        .maxCapability(5L)
        .unit("Gi")
        .mountPoint("/path/to/mount")
        .driverName("test-driver-name")
        .build();
    Assert.assertEquals(VolumeState.NEW, volume.getVolumeState());

    volume.handle(new ValidateVolumeEvent(volume));
    Assert.assertEquals(VolumeState.VALIDATED, volume.getVolumeState());
  }

  @Test
  public void testValidationFailure() throws VolumeException {
    VolumeImpl volume = (VolumeImpl) VolumeBuilder
        .newBuilder().volumeId("test_vol_00000001").build();
    CsiAdaptorClientProtocol mockedClient = Mockito
        .mock(CsiAdaptorClientProtocol.class);
    volume.setClient(mockedClient);

    // NEW -> UNAVAILABLE
    // Simulate a failed API call to the adaptor
    doThrow(new VolumeException("failed")).when(mockedClient).validateVolume();
    volume.handle(new ValidateVolumeEvent(volume));

    try {
      // Verify the countdown did not happen
      GenericTestUtils.waitFor(() ->
          volume.getVolumeState() == VolumeState.VALIDATED, 10, 50);
      Assert.fail("Validate state not reached,"
          + " it should keep waiting until timeout");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof TimeoutException);
      Assert.assertEquals(VolumeState.UNAVAILABLE, volume.getVolumeState());
    }
  }

  @Test
  public void testValidated() throws InvalidVolumeException {
    AtomicInteger validatedTimes = new AtomicInteger(0);
    VolumeImpl volume = (VolumeImpl) VolumeBuilder
        .newBuilder().volumeId("test_vol_00000001").build();
    CsiAdaptorClientProtocol mockedClient = new CsiAdaptorClientProtocol() {
      @Override
      public void validateVolume() {
        validatedTimes.incrementAndGet();
      }

      @Override
      public void controllerPublishVolume() {
        // do nothing
      }
    };
    // The client has a count to memorize how many times being called
    volume.setClient(mockedClient);

    // NEW -> VALIDATED
    Assert.assertEquals(VolumeState.NEW, volume.getVolumeState());
    volume.handle(new ValidateVolumeEvent(volume));
    Assert.assertEquals(VolumeState.VALIDATED, volume.getVolumeState());
    Assert.assertEquals(1, validatedTimes.get());

    // VALIDATED -> VALIDATED
    volume.handle(new ValidateVolumeEvent(volume));
    Assert.assertEquals(VolumeState.VALIDATED, volume.getVolumeState());
    Assert.assertEquals(1, validatedTimes.get());
  }

  @Test
  public void testUnavailableState() throws VolumeException {
    VolumeImpl volume = (VolumeImpl) VolumeBuilder
        .newBuilder().volumeId("test_vol_00000001").build();
    CsiAdaptorClientProtocol mockedClient = Mockito
        .mock(CsiAdaptorClientProtocol.class);
    volume.setClient(mockedClient);

    // NEW -> UNAVAILABLE
    doThrow(new VolumeException("failed")).when(mockedClient)
        .validateVolume();
    Assert.assertEquals(VolumeState.NEW, volume.getVolumeState());
    volume.handle(new ValidateVolumeEvent(volume));
    Assert.assertEquals(VolumeState.UNAVAILABLE, volume.getVolumeState());

    // UNAVAILABLE -> UNAVAILABLE
    volume.handle(new ValidateVolumeEvent(volume));
    Assert.assertEquals(VolumeState.UNAVAILABLE, volume.getVolumeState());

    // UNAVAILABLE -> VALIDATED
    doNothing().when(mockedClient).validateVolume();
    volume.setClient(mockedClient);
    volume.handle(new ValidateVolumeEvent(volume));
    Assert.assertEquals(VolumeState.VALIDATED, volume.getVolumeState());
  }

  @Test
  public void testPublishUnavailableVolume() throws VolumeException {
    VolumeImpl volume = (VolumeImpl) VolumeBuilder
        .newBuilder().volumeId("test_vol_00000001").build();
    CsiAdaptorClientProtocol mockedClient = Mockito
        .mock(CsiAdaptorClientProtocol.class);
    volume.setClient(mockedClient);

    // NEW -> UNAVAILABLE (on validateVolume)
    doThrow(new VolumeException("failed")).when(mockedClient)
        .validateVolume();
    Assert.assertEquals(VolumeState.NEW, volume.getVolumeState());
    volume.handle(new ValidateVolumeEvent(volume));
    Assert.assertEquals(VolumeState.UNAVAILABLE, volume.getVolumeState());

    // UNAVAILABLE -> UNAVAILABLE (on publishVolume)
    volume.handle(new ControllerPublishVolumeEvent(volume));
    // controller publish is not called since the state is UNAVAILABLE
    verify(mockedClient, times(0)).controllerPublishVolume();
    // state remains to UNAVAILABLE
    Assert.assertEquals(VolumeState.UNAVAILABLE, volume.getVolumeState());
  }
}
