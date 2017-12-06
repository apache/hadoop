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
import com.google.common.collect.ImmutableList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.FpgaDiscoverer;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.FPGA_URI;


/**
 * This FPGA resource allocator tends to be used by different FPGA vendor's plugin
 * A "type" parameter is taken into consideration when allocation
 * */
public class FpgaResourceAllocator {

  static final Log LOG = LogFactory.getLog(FpgaResourceAllocator.class);

  private List<FpgaDevice> allowedFpgas = new LinkedList<>();

  //key is resource type of FPGA, vendor plugin supported ID
  private LinkedHashMap<String, List<FpgaDevice>> availableFpga = new LinkedHashMap<>();

  //key is requetor, aka. container ID
  private LinkedHashMap<String, List<FpgaDevice>> usedFpgaByRequestor = new LinkedHashMap<>();

  private Context nmContext;

  @VisibleForTesting
  public HashMap<String, List<FpgaDevice>> getAvailableFpga() {
    return availableFpga;
  }

  @VisibleForTesting
  public List<FpgaDevice> getAllowedFpga() {
    return allowedFpgas;
  }

  public FpgaResourceAllocator(Context ctx) {
    this.nmContext = ctx;
  }

  @VisibleForTesting
  public int getAvailableFpgaCount() {
    int count = 0;
    for (List<FpgaDevice> l : availableFpga.values()) {
      count += l.size();
    }
    return count;
  }

  @VisibleForTesting
  public HashMap<String, List<FpgaDevice>> getUsedFpga() {
    return usedFpgaByRequestor;
  }

  @VisibleForTesting
  public int getUsedFpgaCount() {
    int count = 0;
    for (List<FpgaDevice> l : usedFpgaByRequestor.values()) {
      count += l.size();
    }
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

  public static class FpgaDevice implements Comparable<FpgaDevice>, Serializable {

    private static final long serialVersionUID = 1L;

    private String type;
    private Integer major;
    private Integer minor;
    // IP file identifier. matrix multiplication for instance
    private String IPID;
    // the device name under /dev
    private String devName;
    // the alias device name. Intel use acl number acl0 to acl31
    private String aliasDevName;
    // lspci output's bus number: 02:00.00 (bus:slot.func)
    private String busNum;
    private String temperature;
    private String cardPowerUsage;

    public String getType() {
      return type;
    }

    public Integer getMajor() {
      return major;
    }

    public Integer getMinor() {
      return minor;
    }

    public String getIPID() {
      return IPID;
    }

    public void setIPID(String IPID) {
      this.IPID = IPID;
    }

    public String getDevName() {
      return devName;
    }

    public void setDevName(String devName) {
      this.devName = devName;
    }

    public String getAliasDevName() {
      return aliasDevName;
    }

    public void setAliasDevName(String aliasDevName) {
      this.aliasDevName = aliasDevName;
    }

    public String getBusNum() {
      return busNum;
    }

    public void setBusNum(String busNum) {
      this.busNum = busNum;
    }

    public String getTemperature() {
      return temperature;
    }

    public String getCardPowerUsage() {
      return cardPowerUsage;
    }

    public FpgaDevice(String type, Integer major, Integer minor, String IPID) {
      this.type = type;
      this.major = major;
      this.minor = minor;
      this.IPID = IPID;
    }

    public FpgaDevice(String type, Integer major,
      Integer minor, String IPID, String devName,
        String aliasDevName, String busNum, String temperature, String cardPowerUsage) {
      this.type = type;
      this.major = major;
      this.minor = minor;
      this.IPID = IPID;
      this.devName = devName;
      this.aliasDevName = aliasDevName;
      this.busNum = busNum;
      this.temperature = temperature;
      this.cardPowerUsage = cardPowerUsage;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof FpgaDevice)) {
        return false;
      }
      FpgaDevice other = (FpgaDevice) obj;
      if (other.getType().equals(this.type) &&
          other.getMajor().equals(this.major) &&
          other.getMinor().equals(this.minor)) {
        return true;
      }
      return false;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((type == null) ? 0 : type.hashCode());
      result = prime * result + ((major == null) ? 0 : major.hashCode());
      result = prime * result + ((minor == null) ? 0 : minor.hashCode());
      return result;
    }

    @Override
    public int compareTo(FpgaDevice o) {
      return 0;
    }

    @Override
    public String toString() {
      return "FPGA Device:(Type: " + this.type + ", Major: " +
          this.major + ", Minor: " + this.minor + ", IPID: " + this.IPID + ")";
    }
  }

  public synchronized void addFpga(String type, List<FpgaDevice> list) {
    availableFpga.putIfAbsent(type, new LinkedList<>());
    for (FpgaDevice device : list) {
      if (!allowedFpgas.contains(device)) {
        allowedFpgas.add(device);
        availableFpga.get(type).add(device);
      }
    }
    LOG.info("Add a list of FPGA Devices: " + list);
  }

  public synchronized void updateFpga(String requestor,
      FpgaDevice device, String newIPID) {
    List<FpgaDevice> usedFpgas = usedFpgaByRequestor.get(requestor);
    int index = findMatchedFpga(usedFpgas, device);
    if (-1 != index) {
      usedFpgas.get(index).setIPID(newIPID);
    } else {
      LOG.warn("Failed to update FPGA due to unknown reason " +
          "that no record for this allocated device:" + device);
    }
    LOG.info("Update IPID to " + newIPID +
        " for this allocated device:" + device);
  }

  private synchronized int findMatchedFpga(List<FpgaDevice> devices, FpgaDevice item) {
    int i = 0;
    for (; i < devices.size(); i++) {
      if (devices.get(i) == item) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Assign {@link FpgaAllocation} with preferred IPID, if no, with random FPGAs
   * @param type vendor plugin supported FPGA device type
   * @param count requested FPGA slot count
   * @param container container id
   * @param IPIDPreference allocate slot with this IPID first
   * @return Instance consists two List of allowed and denied {@link FpgaDevice}
   * @throws ResourceHandlerException When failed to allocate or write state store
   * */
  public synchronized FpgaAllocation assignFpga(String type, long count,
      Container container, String IPIDPreference) throws ResourceHandlerException {
    List<FpgaDevice> currentAvailableFpga = availableFpga.get(type);
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
        if ( null != currentAvailableFpga.get(i).getIPID() &&
            currentAvailableFpga.get(i).getIPID().equalsIgnoreCase(IPIDPreference)) {
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
        usedFpgaByRequestor.putIfAbsent(requestor, new LinkedList<>());
        usedFpgaByRequestor.get(requestor).addAll(assignedFpgas);
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
    List<FpgaDevice> usedFpgas = usedFpgaByRequestor.get(requestor);
    if (usedFpgas != null) {
      for (FpgaDevice device : usedFpgas) {
        // Add back to availableFpga
        availableFpga.get(device.getType()).add(device);
      }
      usedFpgaByRequestor.remove(requestor);
    }
  }

}
