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


package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.GpuDeviceInformation;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.GpuDeviceInformationParser;

/**
 * Executes the "nvidia-smi" command and returns an object
 * based on its output.
 *
 */
public class NvidiaBinaryHelper {
  /**
   * command should not run more than 10 sec.
   */
  private static final int MAX_EXEC_TIMEOUT_MS = 10 * 1000;

  /**
   * @param pathOfGpuBinary The path of the binary
   * @return the GpuDeviceInformation parsed from the nvidia-smi output
   * @throws IOException if the binary output is not readable
   * @throws YarnException if the pathOfGpuBinary is null,
   * or the output parse failed
   */
  synchronized GpuDeviceInformation getGpuDeviceInformation(
      String pathOfGpuBinary) throws IOException, YarnException {
    GpuDeviceInformationParser parser = new GpuDeviceInformationParser();

    if (pathOfGpuBinary == null) {
      throw new YarnException(
          "Failed to find GPU discovery executable, please double check "
              + YarnConfiguration.NM_GPU_PATH_TO_EXEC + " setting.");
    }

    String output = Shell.execCommand(new HashMap<>(),
        new String[]{pathOfGpuBinary, "-x", "-q"}, MAX_EXEC_TIMEOUT_MS);
    return parser.parseXml(output);
  }
}
