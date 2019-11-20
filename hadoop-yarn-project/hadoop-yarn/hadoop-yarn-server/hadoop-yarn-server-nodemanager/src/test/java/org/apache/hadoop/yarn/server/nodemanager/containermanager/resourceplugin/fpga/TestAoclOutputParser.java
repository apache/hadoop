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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.IntelFpgaOpenclPlugin.InnerShellExecutor;
import org.junit.Test;

/**
 * Tests for AoclDiagnosticOutputParser.
 */
@SuppressWarnings("checkstyle:linelength")
public class TestAoclOutputParser {

  @Test
  public void testParsing() {
    String output = "------------------------- acl0 -------------------------\n" +
        "Vendor: Nallatech ltd\n" +
        "Phys Dev Name  Status   Information\n" +
        "aclnalla_pcie0Passed   nalla_pcie (aclnalla_pcie0)\n" +
        "                       PCIe dev_id = 2494, bus:slot.func = 02:00.00, Gen3 x8\n" +
        "                       FPGA temperature = 53.1 degrees C.\n" +
        "                       Total Card Power Usage = 31.7 Watts.\n" +
        "                       Device Power Usage = 0.0 Watts.\n" +
        "DIAGNOSTIC_PASSED" +
        "---------------------------------------------------------\n";
    output = output +
        "------------------------- acl1 -------------------------\n" +
        "Vendor: Nallatech ltd\n" +
        "Phys Dev Name  Status   Information\n" +
        "aclnalla_pcie1Passed   nalla_pcie (aclnalla_pcie1)\n" +
        "                       PCIe dev_id = 2495, bus:slot.func = 03:00.00, Gen3 x8\n" +
        "                       FPGA temperature = 43.1 degrees C.\n" +
        "                       Total Card Power Usage = 11.7 Watts.\n" +
        "                       Device Power Usage = 0.0 Watts.\n" +
        "DIAGNOSTIC_PASSED" +
        "---------------------------------------------------------\n";
    output = output +
        "------------------------- acl2 -------------------------\n" +
        "Vendor: Intel(R) Corporation\n" +
        "\n" +
        "Phys Dev Name  Status   Information\n" +
        "\n" +
        "acla10_ref0   Passed   Arria 10 Reference Platform (acla10_ref0)\n" +
        "                       PCIe dev_id = 2494, bus:slot.func = 09:00.00, Gen2 x8\n" +
        "                       FPGA temperature = 50.5781 degrees C.\n" +
        "\n" +
        "DIAGNOSTIC_PASSED\n" +
        "---------------------------------------------------------\n";
    InnerShellExecutor shellExecutor = mock(InnerShellExecutor.class);
    when(shellExecutor.getMajorAndMinorNumber("aclnalla_pcie0"))
        .thenReturn("247:0");
    when(shellExecutor.getMajorAndMinorNumber("aclnalla_pcie1"))
        .thenReturn("247:1");
    when(shellExecutor.getMajorAndMinorNumber("acla10_ref0"))
        .thenReturn("246:0");

    List<FpgaDevice> devices =
        AoclDiagnosticOutputParser.parseDiagnosticOutput(
            output, shellExecutor, "IntelOpenCL");

    assertEquals(3, devices.size());
    assertEquals("IntelOpenCL", devices.get(0).getType());
    assertEquals(247, devices.get(0).getMajor());
    assertEquals(0, devices.get(0).getMinor());
    assertEquals("acl0", devices.get(0).getAliasDevName());

    assertEquals("IntelOpenCL", devices.get(1).getType());
    assertEquals(247, devices.get(1).getMajor());
    assertEquals(1, devices.get(1).getMinor());
    assertEquals("acl1", devices.get(1).getAliasDevName());

    assertEquals("IntelOpenCL", devices.get(2).getType());
    assertEquals(246, devices.get(2).getMajor());
    assertEquals(0, devices.get(2).getMinor());
    assertEquals("acl2", devices.get(2).getAliasDevName());

    // Case 2. check alias map
    assertEquals("acl0", devices.get(0).getAliasDevName());
    assertEquals("acl1", devices.get(1).getAliasDevName());
    assertEquals("acl2", devices.get(2).getAliasDevName());
  }
}
