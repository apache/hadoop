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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.IntelFpgaOpenclPlugin.InnerShellExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class AoclDiagnosticOutputParser {
  private AoclDiagnosticOutputParser() {
    // no instances
  }

  private static final Logger LOG = LoggerFactory.getLogger(
      AoclDiagnosticOutputParser.class);

  /**
   * One real sample output of Intel FPGA SDK 17.0's "aocl diagnose" is as below:
   * "
   * aocl diagnose: Running diagnose from /home/fpga/intelFPGA_pro/17.0/hld/board/nalla_pcie/linux64/libexec
   *
   * ------------------------- acl0 -------------------------
   * Vendor: Nallatech ltd
   *
   * Phys Dev Name  Status   Information
   *
   * aclnalla_pcie0Passed   nalla_pcie (aclnalla_pcie0)
   *                        PCIe dev_id = 2494, bus:slot.func = 02:00.00, Gen3 x8
   *                        FPGA temperature = 54.4 degrees C.
   *                        Total Card Power Usage = 31.7 Watts.
   *                        Device Power Usage = 0.0 Watts.
   *
   * DIAGNOSTIC_PASSED
   * ---------------------------------------------------------
   * "
   *
   * While per Intel's guide, the output(should be outdated or prior SDK version's) is as below:
   *
   * "
   * aocl diagnose: Running diagnostic from ALTERAOCLSDKROOT/board/&lt;board_name&gt;/
   * &lt;platform&gt;/libexec
   * Verified that the kernel mode driver is installed on the host machine.
   * Using board package from vendor: &lt;board_vendor_name&gt;
   * Querying information for all supported devices that are installed on the host
   * machine ...
   *
   * device_name Status Information
   *
   * acl0 Passed &lt;descriptive_board_name&gt;
   *             PCIe dev_id = &lt;device_ID&gt;, bus:slot.func = 02:00.00,
   *               at Gen 2 with 8 lanes.
   *             FPGA temperature=43.0 degrees C.
   * acl1 Passed &lt;descriptive_board_name&gt;
   *             PCIe dev_id = &lt;device_ID&gt;, bus:slot.func = 03:00.00,
   *               at Gen 2 with 8 lanes.
   *             FPGA temperature = 35.0 degrees C.
   *
   * Found 2 active device(s) installed on the host machine, to perform a full
   * diagnostic on a specific device, please run aocl diagnose &lt;device_name&gt;
   *
   * DIAGNOSTIC_PASSED
   * "
   * But this method only support the first output
   * */
  public static List<FpgaDevice> parseDiagnosticOutput(
      String output, InnerShellExecutor shellExecutor, String fpgaType) {
    if (output.contains("DIAGNOSTIC_PASSED")) {
      List<FpgaDevice> devices = new ArrayList<>();
      Matcher headerStartMatcher = Pattern.compile("acl[0-31]")
          .matcher(output);
      Matcher headerEndMatcher = Pattern.compile("(?i)DIAGNOSTIC_PASSED")
          .matcher(output);
      int sectionStartIndex;
      int sectionEndIndex;
      String aliasName;

      while (headerStartMatcher.find()) {
        sectionStartIndex = headerStartMatcher.end();
        String section = null;
        aliasName = headerStartMatcher.group();
        while (headerEndMatcher.find(sectionStartIndex)) {
          sectionEndIndex = headerEndMatcher.start();
          section = output.substring(sectionStartIndex, sectionEndIndex);
          break;
        }

        if (section == null) {
          LOG.warn("Unsupported diagnose output");
          LOG.warn("aocl output is: " + output);
          return Collections.emptyList();
        }

        // devName, \(.*\)
        // busNum, bus:slot.func\s=\s.*,
        // FPGA temperature\s=\s.*
        // Total\sCard\sPower\sUsage\s=\s.*
        String[] fieldRegexes = new String[]{"\\(.*\\)\n",
            "(?i)bus:slot.func\\s=\\s.*,",
            "(?i)FPGA temperature\\s=\\s.*",
            "(?i)Total\\sCard\\sPower\\sUsage\\s=\\s.*"};
        String[] fields = new String[4];
        String tempFieldValue;

        for (int i = 0; i < fieldRegexes.length; i++) {
          Matcher fieldMatcher = Pattern.compile(fieldRegexes[i])
              .matcher(section);
          if (!fieldMatcher.find()) {
            LOG.warn("Couldn't find " + fieldRegexes[i] + " pattern");
            fields[i] = "";
            continue;
          }
          tempFieldValue = fieldMatcher.group().trim();
          if (i == 0) {
            // special case for Device name
            fields[i] = tempFieldValue.substring(1,
                tempFieldValue.length() - 1);
          } else {
            String ss = tempFieldValue.split("=")[1].trim();
            fields[i] = ss.substring(0, ss.length() - 1);
          }
        }

        String majorMinorNumber = shellExecutor
            .getMajorAndMinorNumber(fields[0]);
        if (null != majorMinorNumber) {
          String[] mmn = majorMinorNumber.split(":");

          devices.add(new FpgaDevice(fpgaType,
              Integer.parseInt(mmn[0]),
              Integer.parseInt(mmn[1]),
              aliasName));
        } else {
          LOG.warn("Failed to retrieve major/minor number for device");
        }
      }

      return devices;
    } else {
      LOG.warn("The diagnostic has failed");
      LOG.warn("Output of aocl is: " + output);
      return Collections.emptyList();
    }
  }
}
