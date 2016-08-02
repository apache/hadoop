/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.server.appmaster.web.rest.agent;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class HostInfo {
  private String architecture;
  private String domain;
  private String fqdn;
  private String hardwareisa;
  private String hardwaremodel;
  private String hostname;
  private String id;
  private String interfaces;
  private String ipaddress;
  private String kernel;
  private String kernelmajversion;
  private String kernelrelease;
  private String kernelversion;
  private String macaddress;
  private long memoryfree;
  private long memorysize;
  private List<DiskInfo> mounts = new ArrayList<DiskInfo>();
  private long memorytotal;
  private String netmask;
  private String operatingsystem;
  private String operatingsystemrelease;
  private String osfamily;
  private int physicalprocessorcount;
  private int processorcount;
  private boolean selinux;
  private String swapfree;
  private String swapsize;
  private String timezone;
  private String uptime;
  private long uptime_days;
  private long uptime_hours;


  @JsonProperty("architecture")
  public String getArchitecture() {
    return this.architecture;
  }

  @JsonProperty("architecture")
  public void setArchitecture(String architecture) {
    this.architecture = architecture;
  }

  @JsonProperty("domain")
  public String getDomain() {
    return this.domain;
  }

  @JsonProperty("domain")
  public void setDomain(String domain) {
    this.domain = domain;
  }

  @JsonProperty("fqdn")
  public String getFQDN() {
    return this.fqdn;
  }

  @JsonProperty("fqdn")
  public void setFQDN(String fqdn) {
    this.fqdn = fqdn;
  }

  @JsonProperty("hardwareisa")
  public String getHardwareIsa() {
    return hardwareisa;
  }

  @JsonProperty("hardwareisa")
  public void setHardwareIsa(String hardwareisa) {
    this.hardwareisa = hardwareisa;
  }

  @JsonProperty("hardwaremodel")
  public String getHardwareModel() {
    return this.hardwaremodel;
  }

  @JsonProperty("hardwaremodel")
  public void setHardwareModel(String hardwaremodel) {
    this.hardwaremodel = hardwaremodel;
  }

  @JsonProperty("hostname")
  public String getHostName() {
    return this.hostname;
  }

  @JsonProperty("hostname")
  public void setHostName(String hostname) {
    this.hostname = hostname;
  }

  @JsonProperty("id")
  public String getAgentUserId() {
    return id;
  }

  @JsonProperty("id")
  public void setAgentUserId(String id) {
    this.id = id;
  }

  @JsonProperty("interfaces")
  public String getInterfaces() {
    return this.interfaces;
  }

  @JsonProperty("interfaces")
  public void setInterfaces(String interfaces) {
    this.interfaces = interfaces;
  }

  @JsonProperty("ipaddress")
  public String getIPAddress() {
    return this.ipaddress;
  }

  @JsonProperty("ipaddress")
  public void setIPAddress(String ipaddress) {
    this.ipaddress = ipaddress;
  }

  @JsonProperty("kernel")
  public String getKernel() {
    return this.kernel;
  }

  @JsonProperty("kernel")
  public void setKernel(String kernel) {
    this.kernel = kernel;
  }

  @JsonProperty("kernelmajversion")
  public String getKernelMajVersion() {
    return this.kernelmajversion;
  }

  @JsonProperty("kernelmajversion")
  public void setKernelMajVersion(String kernelmajversion) {
    this.kernelmajversion = kernelmajversion;
  }

  @JsonProperty("kernelrelease")
  public String getKernelRelease() {
    return this.kernelrelease;
  }

  @JsonProperty("kernelrelease")
  public void setKernelRelease(String kernelrelease) {
    this.kernelrelease = kernelrelease;
  }

  @JsonProperty("kernelversion")
  public String getKernelVersion() {
    return this.kernelversion;
  }

  @JsonProperty("kernelversion")
  public void setKernelVersion(String kernelversion) {
    this.kernelversion = kernelversion;
  }

  @JsonProperty("macaddress")
  public String getMacAddress() {
    return this.macaddress;
  }

  @JsonProperty("macaddress")
  public void setMacAddress(String macaddress) {
    this.macaddress = macaddress;
  }

  @JsonProperty("memoryfree")
  public long getFreeMemory() {
    return this.memoryfree;
  }

  @JsonProperty("memoryfree")
  public void setFreeMemory(long memoryfree) {
    this.memoryfree = memoryfree;
  }

  @JsonProperty("memorysize")
  public long getMemorySize() {
    return this.memorysize;
  }

  @JsonProperty("memorysize")
  public void setMemorySize(long memorysize) {
    this.memorysize = memorysize;
  }

  @JsonProperty("mounts")
  public List<DiskInfo> getMounts() {
    return this.mounts;
  }

  @JsonProperty("mounts")
  public void setMounts(List<DiskInfo> mounts) {
    this.mounts = mounts;
  }

  @JsonProperty("memorytotal")
  public long getMemoryTotal() {
    return this.memorytotal;
  }

  @JsonProperty("memorytotal")
  public void setMemoryTotal(long memorytotal) {
    this.memorytotal = memorytotal;
  }

  @JsonProperty("netmask")
  public String getNetMask() {
    return this.netmask;
  }

  @JsonProperty("netmask")
  public void setNetMask(String netmask) {
    this.netmask = netmask;
  }

  @JsonProperty("operatingsystem")
  public String getOS() {
    return this.operatingsystem;
  }

  @JsonProperty("operatingsystem")
  public void setOS(String operatingsystem) {
    this.operatingsystem = operatingsystem;
  }

  @JsonProperty("operatingsystemrelease")
  public String getOSRelease() {
    return this.operatingsystemrelease;
  }

  @JsonProperty("operatingsystemrelease")
  public void setOSRelease(String operatingsystemrelease) {
    this.operatingsystemrelease = operatingsystemrelease;
  }

  @JsonProperty("osfamily")
  public String getOSFamily() {
    return this.osfamily;
  }

  @JsonProperty("osfamily")
  public void setOSFamily(String osfamily) {
    this.osfamily = osfamily;
  }

  @JsonProperty("physicalprocessorcount")
  public int getPhysicalProcessorCount() {
    return this.physicalprocessorcount;
  }

  @JsonProperty("physicalprocessorcount")
  public void setPhysicalProcessorCount(int physicalprocessorcount) {
    this.physicalprocessorcount = physicalprocessorcount;
  }

  @JsonProperty("processorcount")
  public int getProcessorCount() {
    return this.processorcount;
  }

  @JsonProperty("processorcount")
  public void setProcessorCount(int processorcount) {
    this.processorcount = processorcount;
  }

  @JsonProperty("selinux")
  public boolean getSeLinux() {
    return selinux;
  }

  @JsonProperty("selinux")
  public void setSeLinux(boolean selinux) {
    this.selinux = selinux;
  }

  @JsonProperty("swapfree")
  public String getSwapFree() {
    return this.swapfree;
  }

  @JsonProperty("swapfree")
  public void setSwapFree(String swapfree) {
    this.swapfree = swapfree;
  }

  @JsonProperty("swapsize")
  public String getSwapSize() {
    return swapsize;
  }

  @JsonProperty("swapsize")
  public void setSwapSize(String swapsize) {
    this.swapsize = swapsize;
  }

  @JsonProperty("timezone")
  public String getTimeZone() {
    return this.timezone;
  }

  @JsonProperty("timezone")
  public void setTimeZone(String timezone) {
    this.timezone = timezone;
  }

  @JsonProperty("uptime")
  public String getUptime() {
    return this.uptime;
  }

  @JsonProperty("uptime")
  public void setUpTime(String uptime) {
    this.uptime = uptime;
  }

  @JsonProperty("uptime_hours")
  public long getUptimeHours() {
    return this.uptime_hours;
  }

  @JsonProperty("uptime_hours")
  public void setUpTimeHours(long uptime_hours) {
    this.uptime_hours = uptime_hours;
  }

  @JsonProperty("uptime_days")
  public long getUpTimeDays() {
    return this.uptime_days;
  }

  @JsonProperty("uptime_days")
  public void setUpTimeDays(long uptime_days) {
    this.uptime_days = uptime_days;
  }

  private String getDiskString() {
    if (mounts == null) {
      return null;
    }
    StringBuilder ret = new StringBuilder();
    for (DiskInfo diskInfo : mounts) {
      ret.append("(").append(diskInfo.toString()).append(")");
    }
    return ret.toString();
  }

  public String toString() {
    return "[" +
           "hostname=" + this.hostname + "," +
           "fqdn=" + this.fqdn + "," +
           "domain=" + this.domain + "," +
           "architecture=" + this.architecture + "," +
           "processorcount=" + this.processorcount + "," +
           "physicalprocessorcount=" + this.physicalprocessorcount + "," +
           "osname=" + this.operatingsystem + "," +
           "osversion=" + this.operatingsystemrelease + "," +
           "osfamily=" + this.osfamily + "," +
           "memory=" + this.memorytotal + "," +
           "uptime_hours=" + this.uptime_hours + "," +
           "mounts=" + getDiskString() + "]\n";
  }
}
