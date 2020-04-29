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

package org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestGpuDeviceInformationParser {
  private static final String UTF_8 = "UTF-8";
  private static final double DELTA = 1e-6;

  @Rule
  public ExpectedException expected = ExpectedException.none();

  @Test
  public void testParse() throws IOException, YarnException {
    File f = new File("src/test/resources/nvidia-smi-sample-output.xml");
    String s = FileUtils.readFileToString(f, UTF_8);

    GpuDeviceInformationParser parser = new GpuDeviceInformationParser();
    GpuDeviceInformation info = parser.parseXml(s);

    assertEquals("375.66", info.getDriverVersion());
    assertEquals(2, info.getGpus().size());
    assertFirstGpu(info.getGpus().get(0));
    assertSecondGpu(info.getGpus().get(1));
  }

  @Test
  public void testParseExcerpt() throws IOException, YarnException {
    File f = new File("src/test/resources/nvidia-smi-output-excerpt.xml");
    String s = FileUtils.readFileToString(f, UTF_8);

    GpuDeviceInformationParser parser = new GpuDeviceInformationParser();
    GpuDeviceInformation info = parser.parseXml(s);

    assertEquals("375.66", info.getDriverVersion());
    assertEquals(2, info.getGpus().size());
    assertFirstGpu(info.getGpus().get(0));
    assertSecondGpu(info.getGpus().get(1));
  }

  @Test
  public void testParseConsecutivelyWithSameParser()
      throws IOException, YarnException {
    File f = new File("src/test/resources/nvidia-smi-sample-output.xml");
    String s = FileUtils.readFileToString(f, UTF_8);

    for (int i = 0; i < 3; i++) {
      GpuDeviceInformationParser parser = new GpuDeviceInformationParser();
      GpuDeviceInformation info = parser.parseXml(s);

      assertEquals("375.66", info.getDriverVersion());
      assertEquals(2, info.getGpus().size());
      assertFirstGpu(info.getGpus().get(0));
      assertSecondGpu(info.getGpus().get(1));
    }
  }

  @Test
  public void testParseEmptyString() throws YarnException {
    expected.expect(YarnException.class);
    GpuDeviceInformationParser parser = new GpuDeviceInformationParser();
    parser.parseXml("");
  }

  @Test
  public void testParseInvalidRootElement() throws YarnException {
    expected.expect(YarnException.class);
    GpuDeviceInformationParser parser = new GpuDeviceInformationParser();
    parser.parseXml("<nvidia_smiiiii></nvidia_smiiiii");
  }

  @Test
  public void testParseMissingTags() throws IOException, YarnException {
    File f = new File("src/test/resources/nvidia-smi-output-missing-tags.xml");
    String s = FileUtils.readFileToString(f, UTF_8);

    GpuDeviceInformationParser parser = new GpuDeviceInformationParser();
    GpuDeviceInformation info = parser.parseXml(s);

    assertEquals("375.66", info.getDriverVersion());
    assertEquals(1, info.getGpus().size());

    PerGpuDeviceInformation gpu = info.getGpus().get(0);
    assertEquals("N/A", gpu.getProductName());
    assertEquals("N/A", gpu.getUuid());
    assertEquals(-1, gpu.getMinorNumber());
    assertNull(gpu.getGpuMemoryUsage());
    assertNull(gpu.getTemperature());
    assertNull(gpu.getGpuUtilizations());
  }

  @Test
  public void testParseMissingInnerTags() throws IOException, YarnException {
    File f =new File("src/test/resources/nvidia-smi-output-missing-tags2.xml");
    String s = FileUtils.readFileToString(f, UTF_8);

    GpuDeviceInformationParser parser = new GpuDeviceInformationParser();
    GpuDeviceInformation info = parser.parseXml(s);

    assertEquals("375.66", info.getDriverVersion());
    assertEquals(2, info.getGpus().size());

    PerGpuDeviceInformation gpu = info.getGpus().get(0);
    assertEquals("Tesla P100-PCIE-12GB", gpu.getProductName());
    assertEquals("GPU-28604e81-21ec-cc48-6759-bf2648b22e16", gpu.getUuid());
    assertEquals(0, gpu.getMinorNumber());
    assertEquals(-1, gpu.getGpuMemoryUsage().getTotalMemoryMiB());
    assertEquals(-1, (long) gpu.getGpuMemoryUsage().getUsedMemoryMiB());
    assertEquals(-1, (long) gpu.getGpuMemoryUsage().getAvailMemoryMiB());
    assertEquals(0f,
        gpu.getGpuUtilizations().getOverallGpuUtilization(), DELTA);
    assertEquals(Float.MIN_VALUE,
        gpu.getTemperature().getCurrentGpuTemp(), DELTA);
    assertEquals(Float.MIN_VALUE,
        gpu.getTemperature().getMaxGpuTemp(), DELTA);
    assertEquals(Float.MIN_VALUE,
        gpu.getTemperature().getSlowThresholdGpuTemp(),
        DELTA);

    assertSecondGpu(info.getGpus().get(1));
  }

  private void assertFirstGpu(PerGpuDeviceInformation gpu) {
    assertEquals("Tesla P100-PCIE-12GB", gpu.getProductName());
    assertEquals("GPU-28604e81-21ec-cc48-6759-bf2648b22e16", gpu.getUuid());
    assertEquals(0, gpu.getMinorNumber());
    assertEquals(11567, gpu.getGpuMemoryUsage().getTotalMemoryMiB());
    assertEquals(11400, (long) gpu.getGpuMemoryUsage().getUsedMemoryMiB());
    assertEquals(167, (long) gpu.getGpuMemoryUsage().getAvailMemoryMiB());
    assertEquals(33.4f,
        gpu.getGpuUtilizations().getOverallGpuUtilization(), DELTA);
    assertEquals(31f, gpu.getTemperature().getCurrentGpuTemp(), DELTA);
    assertEquals(80f, gpu.getTemperature().getMaxGpuTemp(), DELTA);
    assertEquals(88f, gpu.getTemperature().getSlowThresholdGpuTemp(),
        DELTA);
  }

  private void assertSecondGpu(PerGpuDeviceInformation gpu) {
    assertEquals("Tesla P100-PCIE-12GB_2", gpu.getProductName());
    assertEquals("GPU-46915a82-3fd2-8e11-ae26-a80b607c04f3", gpu.getUuid());
    assertEquals(1, gpu.getMinorNumber());
    assertEquals(12290, gpu.getGpuMemoryUsage().getTotalMemoryMiB());
    assertEquals(11800, (long) gpu.getGpuMemoryUsage().getUsedMemoryMiB());
    assertEquals(490, (long) gpu.getGpuMemoryUsage().getAvailMemoryMiB());
    assertEquals(10.3f,
        gpu.getGpuUtilizations().getOverallGpuUtilization(), DELTA);
    assertEquals(34f, gpu.getTemperature().getCurrentGpuTemp(), DELTA);
    assertEquals(85f, gpu.getTemperature().getMaxGpuTemp(), DELTA);
    assertEquals(82f, gpu.getTemperature().getSlowThresholdGpuTemp(),
        DELTA);
  }
}
