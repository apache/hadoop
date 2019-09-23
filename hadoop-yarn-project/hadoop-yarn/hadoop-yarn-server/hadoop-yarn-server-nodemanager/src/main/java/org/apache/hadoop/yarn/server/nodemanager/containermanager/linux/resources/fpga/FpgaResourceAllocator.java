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


package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.fpga;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.FPGA_URI;

/**
 * This FPGA resource allocator tends to be used by different FPGA vendor's plugin
 * A "type" parameter is taken into consideration when allocation
 * */
public class FpgaResourceAllocator {

  static final Logger LOG = LoggerFactory.
      getLogger(FpgaResourceAllocator.class);

  private List<FpgaDevice> allowedFpgas = new LinkedList<>();

  //key is resource type of FPGA, vendor plugin supported ID
  private Map<String, List<FpgaDevice>> availableFpgas = new HashMap<>();

  //key is the container ID
  private Map<String, List<FpgaDevice>> containerToFpgaMapping =
      new HashMap<>();

  private Context nmContext;

  @VisibleForTesting
  Map<String, List<FpgaDevice>> getAvailableFpga() {
    return availableFpgas;
  }

  @VisibleForTesting
  List<FpgaDevice> getAllowedFpga() {
    return allowedFpgas;
  }

  public FpgaResourceAllocator(Context ctx) {
    this.nmContext = ctx;
  }

  @VisibleForTesting
  int getAvailableFpgaCount() {
    int count = 0;

    count = availableFpgas.values()
      .stream()
      .mapToInt(i -> i.size())
      .sum();

    return count;
  }

  @VisibleForTesting
  Map<String, List<FpgaDevice>> getUsedFpga() {
    return containerToFpgaMapping;
  }

  @VisibleForTesting
  int getUsedFpgaCount() {
    int count = 0;

    count = containerToFpgaMapping.values()
        .stream()
        .mapToInt(i -> i.size())
        .sum();

    return count;
  }

  public static class FpgaAllocation {

    private List<FpgaDevice> allowed = Collections.emptyList();

    private List<FpgaDevice> denied = Collections.emptyList();

    FpgaAllocation(List<FpgaDevice> allowed, List<FpgaDevice> denied) {
      if (allowed != null) {
        this.allowed = ImmutableList.copyOf(allowed);
      }
      if (denied != null) {
        this.denied = ImmutableList.copyOf(denied);
      }
    }

    public List<FpgaDevice> getAllowed() {
      return allowed;
    }

    public List<FpgaDevice> getDenied() {
      return denied;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("\nFpgaAllocation\n\tAllowed:\n");
      for (FpgaDevice device : allowed) {
        sb.append("\t\t");
        sb.append(device + "\n");
      }
      sb.append("\tDenied\n");
      for (FpgaDevice device : denied) {
        sb.append("\t\t");
        sb.append(device + "\n");
      }
      return sb.toString();
    }
  }

  /** A class that represents an FPGA card. */
  public static class FpgaDevice implements Serializable {
    private static final long serialVersionUID = -4678487141824092751L;
    private final String type;
    private final int major;
    private final int minor;

    // the alias device name. Intel use acl number acl0 to acl31
    private final String aliasDevName;

    // IP file identifier. matrix multiplication for instance (mutable)
    private String IPID;
    // SHA-256 hash of the uploaded aocx file (mutable)
    private String aocxHash;

    // cached hash value
    private Integer hashCode;

    public String getType() {
      return type;
    }

    public int getMajor() {
      return major;
    }

    public int getMinor() {
      return minor;
    }

    public String getIPID() {
      return IPID;
    }

    public String getAocxHash() {
      return aocxHash;
    }

    public void setAocxHash(String hash) {
      this.aocxHash = hash;
    }

    public void setIPID(String IPID) {
      this.IPID = IPID;
    }

    public String getAliasDevName() {
      return aliasDevName;
    }

    public FpgaDevice(String type, int major, int minor, String aliasDevName) {
      this.type = Preconditions.checkNotNull(type, "type must not be null");
      this.major = major;
      this.minor = minor;
      this.aliasDevName = Preconditions.checkNotNull(aliasDevName,
          "aliasDevName must not be null");
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      FpgaDevice other = (FpgaDevice) obj;
      if (aliasDevName == null) {
        if (other.aliasDevName != null) {
          return false;
        }
      } else if (!aliasDevName.equals(other.aliasDevName)) {
        return false;
      }
      if (major != other.major) {
        return false;
      }
      if (minor != other.minor) {
        return false;
      }
      if (type == null) {
        if (other.type != null) {
          return false;
        }
      } else if (!type.equals(other.type)) {
        return false;
      }
      return true;
    }

    @Override
    public int hashCode() {
      if (hashCode == null) {
        final int prime = 31;
        int result = 1;

        result = prime * result + major;
        result = prime * result + type.hashCode();
        result = prime * result + minor;
        result = prime * result + aliasDevName.hashCode();

        hashCode = result;
      }

      return hashCode;
    }

    @Override
    public String toString() {
      return "FPGA Device:(Type: " + this.type + ", Major: " +
          this.major + ", Minor: " + this.minor + ", IPID: " +
          this.IPID + ", Hash: " + this.aocxHash + ")";
    }
  }

  // called once during initialization
  public synchronized void addFpgaDevices(String type, List<FpgaDevice> list) {
    availableFpgas.putIfAbsent(type, new LinkedList<>());
    List<FpgaDevice> fpgaDevices = new LinkedList<>();

    for (FpgaDevice device : list) {
      if (!allowedFpgas.contains(device)) {
        fpgaDevices.add(device);
        availableFpgas.get(type).add(device);
      } else {
        LOG.warn("Duplicate device found: " + device + ". Ignored");
      }
    }

    allowedFpgas = ImmutableList.copyOf(fpgaDevices);
    LOG.info("Added a list of FPGA Devices: " + allowedFpgas);
  }

  public synchronized void updateFpga(String requestor,
      FpgaDevice device, String newIPID, String newHash) {
    device.setIPID(newIPID);
    device.setAocxHash(newHash);
    LOG.info("Update IPID to " + newIPID +
        " for this allocated device: " + device);
    LOG.info("Update IP hash to " + newHash);
  }

  /**
   * Assign {@link FpgaAllocation} with preferred IPID, if no, with random FPGAs
   * @param type vendor plugin supported FPGA device type
   * @param count requested FPGA slot count
   * @param container container id
   * @param ipidHash hash of the localized aocx file
   * @return Instance consists two List of allowed and denied {@link FpgaDevice}
   * @throws ResourceHandlerException When failed to allocate or write state store
   * */
  public synchronized FpgaAllocation assignFpga(String type, long count,
      Container container, String ipidHash) throws ResourceHandlerException {
    List<FpgaDevice> currentAvailableFpga = availableFpgas.get(type);

    String requestor = container.getContainerId().toString();
    if (null == currentAvailableFpga) {
      throw new ResourceHandlerException("No such type of FPGA resource available: " + type);
    }
    if (count < 0 || count > currentAvailableFpga.size()) {
      throw new ResourceHandlerException("Invalid FPGA request count or not enough, requested:" +
          count + ", available:" + getAvailableFpgaCount());
    }
    if (count > 0) {
      // Allocate devices with matching IP first, then any device is ok
      List<FpgaDevice> assignedFpgas = new LinkedList<>();
      int matchIPCount = 0;
      for (int i = 0; i < currentAvailableFpga.size(); i++) {
        String deviceIPIDhash = currentAvailableFpga.get(i).getAocxHash();
        if (deviceIPIDhash != null &&
            deviceIPIDhash.equalsIgnoreCase(ipidHash)) {
          assignedFpgas.add(currentAvailableFpga.get(i));
          currentAvailableFpga.remove(i);
          matchIPCount++;
        }
      }
      int remaining = (int) count - matchIPCount;
      while (remaining > 0) {
        assignedFpgas.add(currentAvailableFpga.remove(0));
        remaining--;
      }

      // Record in state store if we allocated anything
      if (!assignedFpgas.isEmpty()) {
        try {
          nmContext.getNMStateStore().storeAssignedResources(container,
              FPGA_URI, new LinkedList<>(assignedFpgas));
        } catch (IOException e) {
          // failed, give the allocation back
          currentAvailableFpga.addAll(assignedFpgas);
          throw new ResourceHandlerException(e);
        }

        // update state store success, update internal used FPGAs
        containerToFpgaMapping.putIfAbsent(requestor, new LinkedList<>());
        containerToFpgaMapping.get(requestor).addAll(assignedFpgas);
      }

      return new FpgaAllocation(assignedFpgas, currentAvailableFpga);
    }
    return new FpgaAllocation(null, allowedFpgas);
  }

  public synchronized void recoverAssignedFpgas(ContainerId containerId) throws ResourceHandlerException {
    Container c = nmContext.getContainers().get(containerId);
    if (null == c) {
      throw new ResourceHandlerException(
          "This shouldn't happen, cannot find container with id="
              + containerId);
    }

    for (Serializable fpgaDevice :
        c.getResourceMappings().getAssignedResources(FPGA_URI)) {
      if (!(fpgaDevice instanceof FpgaDevice)) {
        throw new ResourceHandlerException(
            "Trying to recover allocated FPGA devices, however it"
                + " is not FpgaDevice type, this shouldn't happen");
      }

      // Make sure it is in allowed FPGA device.
      if (!allowedFpgas.contains(fpgaDevice)) {
        throw new ResourceHandlerException("Try to recover FpgaDevice = " + fpgaDevice
            + " however it is not in allowed device list:" + StringUtils
            .join(";", allowedFpgas));
      }

      // Make sure it is not occupied by anybody else
      Iterator<Map.Entry<String, List<FpgaDevice>>> iterator =
          getUsedFpga().entrySet().iterator();
      while (iterator.hasNext()) {
        if (iterator.next().getValue().contains(fpgaDevice)) {
          throw new ResourceHandlerException("Try to recover FpgaDevice = " + fpgaDevice
              + " however it is already assigned to others");
        }
      }
      getUsedFpga().putIfAbsent(containerId.toString(), new LinkedList<>());
      getUsedFpga().get(containerId.toString()).add((FpgaDevice) fpgaDevice);
      // remove them from available list
      getAvailableFpga().get(((FpgaDevice) fpgaDevice).getType()).remove(fpgaDevice);
    }
  }

  public synchronized void cleanupAssignFpgas(String requestor) {
    List<FpgaDevice> usedFpgas = containerToFpgaMapping.get(requestor);
    if (usedFpgas != null) {
      for (FpgaDevice device : usedFpgas) {
        // Add back to availableFpga
        availableFpgas.get(device.getType()).add(device);
      }
      containerToFpgaMapping.remove(requestor);
    }
  }
}
