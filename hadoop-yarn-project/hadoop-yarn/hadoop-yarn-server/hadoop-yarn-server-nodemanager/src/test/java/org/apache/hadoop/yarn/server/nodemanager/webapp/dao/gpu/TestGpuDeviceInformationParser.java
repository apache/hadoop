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
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestGpuDeviceInformationParser {
  @Test
  public void testParse() throws IOException, YarnException {
    File f = new File("src/test/resources/nvidia-smi-sample-xml-output");
    String s = FileUtils.readFileToString(f, "UTF-8");

    GpuDeviceInformationParser parser = new GpuDeviceInformationParser();

    GpuDeviceInformation info = parser.parseXml(s);
    Assert.assertEquals("375.66", info.getDriverVersion());
    Assert.assertEquals(2, info.getGpus().size());
    PerGpuDeviceInformation gpu1 = info.getGpus().get(1);
    Assert.assertEquals("Tesla P100-PCIE-12GB", gpu1.getProductName());
    Assert.assertEquals(12193, gpu1.getGpuMemoryUsage().getTotalMemoryMiB());
    Assert.assertEquals(10.3f,
        gpu1.getGpuUtilizations().getOverallGpuUtilization(), 1e-6);
    Assert.assertEquals(34f, gpu1.getTemperature().getCurrentGpuTemp(), 1e-6);
    Assert.assertEquals(85f, gpu1.getTemperature().getMaxGpuTemp(), 1e-6);
    Assert.assertEquals(82f, gpu1.getTemperature().getSlowThresholdGpuTemp(),
        1e-6);
  }
}
