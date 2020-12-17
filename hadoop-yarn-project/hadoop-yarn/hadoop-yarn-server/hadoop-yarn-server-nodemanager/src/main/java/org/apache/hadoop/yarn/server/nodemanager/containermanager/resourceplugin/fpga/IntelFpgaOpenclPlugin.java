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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Intel FPGA for OpenCL plugin.
 * The key points are:
 * 1. It uses Intel's toolchain "aocl" to discover devices/reprogram IP
 *    to the device before container launch to achieve a quickest
 *    reprogramming path
 * 2. It avoids reprogramming by maintaining a mapping of device to FPGA IP ID
 * 3. It assume IP file is distributed to container directory
 */
public class IntelFpgaOpenclPlugin implements AbstractFpgaVendorPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(
      IntelFpgaOpenclPlugin.class);

  private boolean initialized = false;
  private InnerShellExecutor shell;

  private static final String DEFAULT_BINARY_NAME = "aocl";

  private static final String ALTERAOCLSDKROOT_NAME = "ALTERAOCLSDKROOT";

  private Function<String, String> envProvider = System::getenv;

  private String pathToExecutable = null;

  @VisibleForTesting
  void setInnerShellExecutor(InnerShellExecutor shellExecutor) {
    this.shell = shellExecutor;
  }

  @VisibleForTesting
  String getPathToExecutable() {
    return pathToExecutable;
  }

  @VisibleForTesting
  void setEnvProvider(Function<String, String> envProvider) {
    this.envProvider = envProvider;
  }

  public IntelFpgaOpenclPlugin() {
    this.shell = new InnerShellExecutor();
  }

  public String getDefaultPathToExecutable() {
    return envProvider.apply(ALTERAOCLSDKROOT_NAME);
  }

  /**
   * Check the Intel FPGA for OpenCL toolchain.
   * */
  @Override
  public boolean initPlugin(Configuration config) {
    if (initialized) {
      return true;
    }

    // Find the proper toolchain, mainly aocl
    String pluginDefaultBinaryName = DEFAULT_BINARY_NAME;
    String executable = config.get(YarnConfiguration.NM_FPGA_PATH_TO_EXEC,
        pluginDefaultBinaryName);

    // Validate file existence
    File binaryPath = new File(executable);
    if (!binaryPath.exists()) {
      // When binary not exist, fail
      LOG.warn("Failed to find FPGA discoverer executable configured in " +
          YarnConfiguration.NM_FPGA_PATH_TO_EXEC +
          ", please check! Try default path");
      executable = pluginDefaultBinaryName;
      // Try to find in plugin's preferred path
      String pluginDefaultPreferredPath = getDefaultPathToExecutable();
      if (null == pluginDefaultPreferredPath) {
        LOG.warn("Failed to find FPGA discoverer executable from system "
            + " environment " + ALTERAOCLSDKROOT_NAME +
            ", please check your environment!");
      } else {
        binaryPath = new File(pluginDefaultPreferredPath + "/bin",
            pluginDefaultBinaryName);
        if (binaryPath.exists()) {
          executable = binaryPath.getAbsolutePath();
          LOG.info("Succeed in finding FPGA discoverer executable: " +
              executable);
        } else {
          executable = pluginDefaultBinaryName;
          LOG.warn("Failed to find FPGA discoverer executable in " +
              pluginDefaultPreferredPath +
              ", file doesn't exists! Use default binary" + executable);
        }
      }
    }

    pathToExecutable = executable;

    if (!diagnose(10*1000)) {
      LOG.warn("Intel FPGA for OpenCL diagnose failed!");
      initialized = false;
    } else {
      initialized = true;
    }
    return initialized;
  }

  @Override
  public List<FpgaDevice> discover(int timeout) {
    List<FpgaDevice> list = new LinkedList<>();
    String output;
    output = getDiagnoseInfo(timeout);
    if (null == output) {
      return list;
    }

    list = AoclDiagnosticOutputParser.parseDiagnosticOutput(output,
        shell, getFpgaType());

    return list;
  }

  /**
   *  Helper class to run aocl diagnose &amp; determine major/minor numbers.
   */
  public static class InnerShellExecutor {

    // ls /dev/<devName>
    // return a string in format <major:minor>
    public String getMajorAndMinorNumber(String devName) {
      String output = null;
      Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(
          new String[]{"stat", "-c", "%t:%T", "/dev/" + devName});
      try {
        LOG.debug("Get FPGA major-minor numbers from /dev/{}", devName);
        shexec.execute();
        String[] strs = shexec.getOutput().trim().split(":");
        LOG.debug("stat output:{}", shexec.getOutput());
        output = Integer.parseInt(strs[0], 16) + ":" +
            Integer.parseInt(strs[1], 16);
      } catch (IOException e) {
        LOG.warn("Failed to get major-minor number from reading /dev/" +
            devName);
        LOG.warn("Command output:" + shexec.getOutput() + ", exit code: " +
            shexec.getExitCode(), e);
      }
      return output;
    }

    public String runDiagnose(String binary, int timeout) {
      String output = null;
      Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(
          new String[]{binary, "diagnose"}, null, null, timeout);
      try {
        shexec.execute();
      } catch (IOException e) {
        // aocl diagnose exit code is 1 even it success.
        // we ignore it because we only wants the output
        String msg =
            "Failed to execute " + binary + " diagnose, exception message:" + e
                .getMessage() +", output:" + output + ", continue ...";
        LOG.warn(msg);
        LOG.debug("{}", shexec.getOutput());
      }
      return shexec.getOutput();
    }
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
  public String retrieveIPfilePath(String id, String dstDir,
      Map<Path, List<String>> localizedResources) {
    // Assume .aocx IP file is distributed by DS to local dir
    String ipFilePath = null;

    LOG.info("Got environment: " + id +
        ", search IP file in localized resources");
    if (null == id || id.isEmpty()) {
      LOG.warn("IP_ID environment is empty, skip downloading");
      return null;
    }

    if (localizedResources != null) {
      Optional<Path> aocxPath = localizedResources
          .keySet()
          .stream()
          .filter(path -> matchesIpid(path, id))
          .findFirst();

      if (aocxPath.isPresent()) {
        ipFilePath = aocxPath.get().toString();
        LOG.info("Found: {}", ipFilePath);
      } else {
        LOG.warn("Requested IP file not found");
      }
    } else {
      LOG.warn("Localized resource is null!");
    }

    return ipFilePath;
  }

  private boolean matchesIpid(Path p, String id) {
    return p.getName().toLowerCase().equals(id.toLowerCase() + ".aocx");
  }

  /**
   * Program one device.
   * It's ok for the offline "aocl program" failed because the application will
   * always invoke API to program.
   * The reason we do offline reprogramming is to make the application's
   * program process faster.
   * @param ipPath the absolute path to the aocx IP file
   * @param device Fpga device object which represents the card
   * @return false if programming the card fails
   * */
  @Override
  public boolean configureIP(String ipPath, FpgaDevice device) {
    // perform offline program the IP to get a quickest reprogramming sequence
    // we need a mapping of "major:minor" to "acl0" to
    // issue command "aocl program <acl0> <ipPath>"
    Shell.ShellCommandExecutor shexec;
    String aclName;
    aclName = device.getAliasDevName();
    shexec = new Shell.ShellCommandExecutor(
        new String[]{this.pathToExecutable, "program", aclName, ipPath});
    try {
      shexec.execute();
      if (0 == shexec.getExitCode()) {
        LOG.debug("{}", shexec.getOutput());
        LOG.info("Intel aocl program " + ipPath + " to " +
            aclName + " successfully");
      } else {
        LOG.error("Device programming failed, aocl output is:");
        LOG.error(shexec.getOutput());
        return false;
      }
    } catch (IOException e) {
      LOG.error("Intel aocl program " + ipPath + " to " +
          aclName + " failed!", e);
      LOG.error("Aocl output: " + shexec.getOutput());
      return false;
    }
    return true;
  }
}
