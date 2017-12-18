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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.fpga.FpgaResourceAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Intel FPGA for OpenCL plugin.
 * The key points are:
 * 1. It uses Intel's toolchain "aocl" to discover devices/reprogram IP to the device
 *    before container launch to achieve a quickest reprogramming path
 * 2. It avoids reprogramming by maintaining a mapping of device to FPGA IP ID
 * 3. It assume IP file is distributed to container directory
 */
public class IntelFpgaOpenclPlugin implements AbstractFpgaVendorPlugin {
  public static final Logger LOG = LoggerFactory.getLogger(
      IntelFpgaOpenclPlugin.class);

  private boolean initialized = false;
  private Configuration conf;
  private InnerShellExecutor shell;

  protected static final String DEFAULT_BINARY_NAME = "aocl";

  protected static final String ALTERAOCLSDKROOT_NAME = "ALTERAOCLSDKROOT";

  private String pathToExecutable = null;

  // a mapping of major:minor number to acl0-31
  private Map<String, String> aliasMap;

  public IntelFpgaOpenclPlugin() {
    this.shell = new InnerShellExecutor();
  }

  public String getDefaultBinaryName() {
    return DEFAULT_BINARY_NAME;
  }

  public String getDefaultPathToExecutable() {
    return System.getenv(ALTERAOCLSDKROOT_NAME);
  }

  public static String getDefaultPathEnvName() {
    return ALTERAOCLSDKROOT_NAME;
  }

  @VisibleForTesting
  public String getPathToExecutable() {
    return pathToExecutable;
  }

  public void setPathToExecutable(String pathToExecutable) {
    this.pathToExecutable = pathToExecutable;
  }

  @VisibleForTesting
  public void setShell(InnerShellExecutor shell) {
    this.shell = shell;
  }

  public Map<String, String> getAliasMap() {
    return aliasMap;
  }

  /**
   * Check the Intel FPGA for OpenCL toolchain
   * */
  @Override
  public boolean initPlugin(Configuration conf) {
    this.aliasMap = new HashMap<>();
    if (this.initialized) {
      return true;
    }
    // Find the proper toolchain, mainly aocl
    String pluginDefaultBinaryName = getDefaultBinaryName();
    String pathToExecutable = conf.get(YarnConfiguration.NM_FPGA_PATH_TO_EXEC,
        "");
    if (pathToExecutable.isEmpty()) {
      pathToExecutable = pluginDefaultBinaryName;
    }
    // Validate file existence
    File binaryPath = new File(pathToExecutable);
    if (!binaryPath.exists()) {
      // When binary not exist, fail
      LOG.warn("Failed to find FPGA discoverer executable configured in " +
          YarnConfiguration.NM_FPGA_PATH_TO_EXEC +
          ", please check! Try default path");
      pathToExecutable = pluginDefaultBinaryName;
      // Try to find in plugin's preferred path
      String pluginDefaultPreferredPath = getDefaultPathToExecutable();
      if (null == pluginDefaultPreferredPath) {
        LOG.warn("Failed to find FPGA discoverer executable from system environment " +
            getDefaultPathEnvName()+
            ", please check your environment!");
      } else {
        binaryPath = new File(pluginDefaultPreferredPath + "/bin", pluginDefaultBinaryName);
        if (binaryPath.exists()) {
          pathToExecutable = pluginDefaultPreferredPath;
        } else {
          pathToExecutable = pluginDefaultBinaryName;
          LOG.warn("Failed to find FPGA discoverer executable in " +
              pluginDefaultPreferredPath + ", file doesn't exists! Use default binary" + pathToExecutable);
        }
      }
    }
    setPathToExecutable(pathToExecutable);
    if (!diagnose(10*1000)) {
      LOG.warn("Intel FPGA for OpenCL diagnose failed!");
      this.initialized = false;
    } else {
      this.initialized = true;
    }
    return this.initialized;
  }

  @Override
  public List<FpgaResourceAllocator.FpgaDevice> discover(int timeout) {
    List<FpgaResourceAllocator.FpgaDevice> list = new LinkedList<>();
    String output;
    output = getDiagnoseInfo(timeout);
    if (null == output) {
      return list;
    }
    parseDiagnoseInfo(output, list);
    return list;
  }

  public static class InnerShellExecutor {

    // ls /dev/<devName>
    // return a string in format <major:minor>
    public String getMajorAndMinorNumber(String devName) {
      String output = null;
      Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(
          new String[]{"stat", "-c", "%t:%T", "/dev/" + devName});
      try {
        LOG.debug("Get FPGA major-minor numbers from /dev/" + devName);
        shexec.execute();
        String[] strs = shexec.getOutput().trim().split(":");
        LOG.debug("stat output:" + shexec.getOutput());
        output = Integer.parseInt(strs[0], 16) + ":" + Integer.parseInt(strs[1], 16);
      } catch (IOException e) {
        String msg =
            "Failed to get major-minor number from reading /dev/" + devName;
        LOG.warn(msg);
        LOG.debug("Command output:" + shexec.getOutput() + ", exit code:" +
            shexec.getExitCode());
      }
      return output;
    }

    public String runDiagnose(String binary, int timeout) {
      String output = null;
      Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(
          new String[]{binary, "diagnose"});
      try {
        shexec.execute();
      } catch (IOException e) {
        // aocl diagnose exit code is 1 even it success.
        // we ignore it because we only wants the output
        String msg =
            "Failed to execute " + binary + " diagnose, exception message:" + e
                .getMessage() +", output:" + output + ", continue ...";
        LOG.warn(msg);
        LOG.debug(shexec.getOutput());
      }
      return shexec.getOutput();
    }

  }

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
  public void parseDiagnoseInfo(String output, List<FpgaResourceAllocator.FpgaDevice> list) {
    if (output.contains("DIAGNOSTIC_PASSED")) {
      Matcher headerStartMatcher = Pattern.compile("acl[0-31]").matcher(output);
      Matcher headerEndMatcher = Pattern.compile("(?i)DIAGNOSTIC_PASSED").matcher(output);
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
        if (null == section) {
          LOG.warn("Unsupported diagnose output");
          return;
        }
        // devName, \(.*\)
        // busNum, bus:slot.func\s=\s.*,
        // FPGA temperature\s=\s.*
        // Total\sCard\sPower\sUsage\s=\s.*
        String[] fieldRegexes = new String[]{"\\(.*\\)\n", "(?i)bus:slot.func\\s=\\s.*,",
            "(?i)FPGA temperature\\s=\\s.*", "(?i)Total\\sCard\\sPower\\sUsage\\s=\\s.*"};
        String[] fields = new String[4];
        String tempFieldValue;
        for (int i = 0; i < fieldRegexes.length; i++) {
          Matcher fieldMatcher = Pattern.compile(fieldRegexes[i]).matcher(section);
          if (!fieldMatcher.find()) {
            LOG.warn("Couldn't find " + fieldRegexes[i] + " pattern");
            fields[i] = "";
            continue;
          }
          tempFieldValue = fieldMatcher.group().trim();
          if (i == 0) {
            // special case for Device name
            fields[i] = tempFieldValue.substring(1, tempFieldValue.length() - 1);
          } else {
            String ss = tempFieldValue.split("=")[1].trim();
            fields[i] = ss.substring(0, ss.length() - 1);
          }
        }
        String majorMinorNumber = this.shell.getMajorAndMinorNumber(fields[0]);
        if (null != majorMinorNumber) {
          String[] mmn = majorMinorNumber.split(":");
          this.aliasMap.put(majorMinorNumber, aliasName);
          list.add(new FpgaResourceAllocator.FpgaDevice(getFpgaType(),
              Integer.parseInt(mmn[0]),
              Integer.parseInt(mmn[1]), null,
              fields[0], aliasName, fields[1], fields[2], fields[3]));
        }
      }// end while
    }// end if
  }

  public String getDiagnoseInfo(int timeout) {
    return this.shell.runDiagnose(this.pathToExecutable,timeout);
  }

  @Override
  public boolean diagnose(int timeout) {
    String output = getDiagnoseInfo(timeout);
    if (null != output && output.contains("DIAGNOSTIC_PASSED")) {
      return true;
    }
    return false;
  }

  /**
   * this is actually the opencl platform type
   * */
  @Override
  public String getFpgaType() {
    return "IntelOpenCL";
  }

  @Override
  public String downloadIP(String id, String dstDir, Map<Path, List<String>> localizedResources) {
    // Assume .aocx IP file is distributed by DS to local dir
    String r = "";
    Path path;
    LOG.info("Got environment: " + id + ", search IP file in localized resources");
    if (null == id || id.isEmpty()) {
      LOG.warn("IP_ID environment is empty, skip downloading");
      return r;
    }
    if (localizedResources != null) {
      for (Map.Entry<Path, List<String>> resourceEntry :
          localizedResources.entrySet()) {
        path = resourceEntry.getKey();
        LOG.debug("Check:" + path.toUri().toString());
        if (path.getName().toLowerCase().contains(id.toLowerCase()) && path.getName().endsWith(".aocx")) {
          r = path.toUri().toString();
          LOG.debug("Found: " + r);
          break;
        }
      }
    } else {
      LOG.warn("Localized resource is null!");
    }
    return r;
  }

  /**
   * Program one device.
   * It's ok for the offline "aocl program" failed because the application will always invoke API to program
   * The reason we do offline reprogramming is to make the application's program process faster
   * @param ipPath the absolute path to the aocx IP file
   * @param majorMinorNumber major:minor string
   * @return True or False
   * */
  @Override
  public boolean configureIP(String ipPath, String majorMinorNumber) {
    // perform offline program the IP to get a quickest reprogramming sequence
    // we need a mapping of "major:minor" to "acl0" to issue command "aocl program <acl0> <ipPath>"
    Shell.ShellCommandExecutor shexec;
    String aclName;
    aclName = this.aliasMap.get(majorMinorNumber);
    shexec = new Shell.ShellCommandExecutor(
        new String[]{this.pathToExecutable, "program", aclName, ipPath});
    try {
      shexec.execute();
      if (0 == shexec.getExitCode()) {
        LOG.debug(shexec.getOutput());
        LOG.info("Intel aocl program " + ipPath + " to " + aclName + " successfully");
      } else {
        return false;
      }
    } catch (IOException e) {
      LOG.error("Intel aocl program " + ipPath + " to " + aclName + " failed!");
      e.printStackTrace();
      return false;
    }
    return true;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }
}
