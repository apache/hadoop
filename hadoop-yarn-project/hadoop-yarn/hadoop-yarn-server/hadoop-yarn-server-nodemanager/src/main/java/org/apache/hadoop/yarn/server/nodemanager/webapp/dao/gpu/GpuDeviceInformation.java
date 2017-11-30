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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * All GPU Device Information in the system, fetched from nvidia-smi.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@XmlRootElement(name = "nvidia_smi_log")
public class GpuDeviceInformation {
  List<PerGpuDeviceInformation> gpus;

  String driverVersion = "N/A";

  // More fields like topology information could be added when needed.
  // ...

  @javax.xml.bind.annotation.XmlElement(name = "gpu")
  public List<PerGpuDeviceInformation> getGpus() {
    return gpus;
  }

  public void setGpus(List<PerGpuDeviceInformation> gpus) {
    this.gpus = gpus;
  }

  @javax.xml.bind.annotation.XmlElement(name = "driver_version")
  public String getDriverVersion() {
    return driverVersion;
  }

  public void setDriverVersion(String driverVersion) {
    this.driverVersion = driverVersion;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("=== Gpus in the system ===\n").append("\tDriver Version:").append(
        getDriverVersion()).append("\n");

    if (gpus != null) {
      for (PerGpuDeviceInformation gpu : gpus) {
        sb.append("\t").append(gpu.toString()).append("\n");
      }
    }
    return sb.toString();
  }
}
