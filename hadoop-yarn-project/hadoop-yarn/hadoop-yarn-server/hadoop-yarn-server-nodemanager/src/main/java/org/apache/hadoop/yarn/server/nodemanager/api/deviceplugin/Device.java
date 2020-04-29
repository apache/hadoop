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

package org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represent one "device" resource.
 * */
public final class Device implements Serializable, Comparable {

  private static final long serialVersionUID = -7270474563684671656L;

  /**
   * An plugin specified index number.
   * Must set. Recommend starting from 0
   * */
  private final int id;

  /**
   * The device node like "/dev/devname".
   * Optional
   * */
  private final String devPath;

  /**
   * The major device number.
   * Optional
   * */
  private final int majorNumber;

  /**
   * The minor device number.
   * Optional
   * */
  private final int minorNumber;

  /**
   * PCI Bus ID in format.
   * [[[[&lt;domain&gt;]:]&lt;bus&gt;]:][&lt;slot&gt;][.[&lt;func&gt;]].
   * Optional. Can get from "lspci -D" in Linux
   * */
  private final String busID;

  /**
   * Is healthy or not.
   * false by default
   * */
  private boolean isHealthy;

  /**
   * Plugin customized status info.
   * Optional
   * */
  private String status;

  /**
   * Private constructor.
   * @param builder
   */
  private Device(Builder builder) {
    if (builder.id == -1) {
      throw new IllegalArgumentException("Please set the id for Device");
    }
    this.id = builder.id;
    this.devPath = builder.devPath;
    this.majorNumber = builder.majorNumber;
    this.minorNumber = builder.minorNumber;
    this.busID = builder.busID;
    this.isHealthy = builder.isHealthy;
    this.status = builder.status;
  }

  public int getId() {
    return id;
  }

  public String getDevPath() {
    return devPath;
  }

  public int getMajorNumber() {
    return majorNumber;
  }

  public int getMinorNumber() {
    return minorNumber;
  }

  public String getBusID() {
    return busID;
  }

  public boolean isHealthy() {
    return isHealthy;
  }

  public String getStatus() {
    return status;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Device device = (Device) o;
    return id == device.getId()
        && Objects.equals(devPath, device.getDevPath())
        && majorNumber == device.getMajorNumber()
        && minorNumber == device.getMinorNumber()
        && Objects.equals(busID, device.getBusID());
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, devPath, majorNumber, minorNumber, busID);
  }

  @Override
  public int compareTo(Object o) {
    if (o == null || (!(o instanceof Device))) {
      return -1;
    }

    Device other = (Device) o;

    int result = Integer.compare(id, other.getId());
    if (0 != result) {
      return result;
    }

    result = Integer.compare(majorNumber, other.getMajorNumber());
    if (0 != result) {
      return result;
    }

    result = Integer.compare(minorNumber, other.getMinorNumber());
    if (0 != result) {
      return result;
    }

    result = devPath.compareTo(other.getDevPath());
    if (0 != result) {
      return result;
    }

    return busID.compareTo(other.getBusID());
  }

  @Override
  public String toString() {
    return "(" + getId() + ", " + getDevPath() + ", "
        + getMajorNumber() + ":" + getMinorNumber() + ")";
  }

  /**
   * Builder for Device.
   * */
  public final static class Builder {
    // default -1 representing the value is not set
    private int id = -1;
    private String devPath = "";
    private int majorNumber = -1;
    private int minorNumber = -1;
    private String busID = "";
    private boolean isHealthy;
    private String status = "";

    public static Builder newInstance() {
      return new Builder();
    }

    public Device build() {
      return new Device(this);
    }

    public Builder setId(int i) {
      this.id = i;
      return this;
    }

    public Builder setDevPath(String dp) {
      this.devPath = dp;
      return this;
    }

    public Builder setMajorNumber(int maN) {
      this.majorNumber = maN;
      return this;
    }

    public Builder setMinorNumber(int miN) {
      this.minorNumber = miN;
      return this;
    }

    public Builder setBusID(String bI) {
      this.busID = bI;
      return this;
    }

    public Builder setHealthy(boolean healthy) {
      isHealthy = healthy;
      return this;
    }

    public Builder setStatus(String s) {
      this.status = s;
      return this;
    }

  }
}
