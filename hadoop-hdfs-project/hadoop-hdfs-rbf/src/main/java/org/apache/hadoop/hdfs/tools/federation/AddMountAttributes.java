/*
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

package org.apache.hadoop.hdfs.tools.federation;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;

/**
 * Add mount entry attributes to be used by Router admin.
 */
public class AddMountAttributes {

  private String mount;
  private String[] nss;
  private String[] destinations;
  private boolean readonly;
  private boolean faultTolerant;
  private DestinationOrder order;
  private RouterAdmin.ACLEntity aclInfo;
  private int paramIndex;

  public String getMount() {
    return mount;
  }

  public void setMount(String mount) {
    this.mount = mount;
  }

  public String[] getNss() {
    return nss;
  }

  public void setNss(String[] nss) {
    this.nss = nss;
  }

  public String[] getDestinations() {
    return destinations;
  }

  public void setDestinations(String[] destinations) {
    this.destinations = destinations;
  }

  public boolean isReadonly() {
    return readonly;
  }

  public void setReadonly(boolean readonly) {
    this.readonly = readonly;
  }

  public boolean isFaultTolerant() {
    return faultTolerant;
  }

  public void setFaultTolerant(boolean faultTolerant) {
    this.faultTolerant = faultTolerant;
  }

  public DestinationOrder getOrder() {
    return order;
  }

  public void setOrder(DestinationOrder order) {
    this.order = order;
  }

  public RouterAdmin.ACLEntity getAclInfo() {
    return aclInfo;
  }

  public void setAclInfo(RouterAdmin.ACLEntity aclInfo) {
    this.aclInfo = aclInfo;
  }

  public int getParamIndex() {
    return paramIndex;
  }

  public void setParamIndex(int paramIndex) {
    this.paramIndex = paramIndex;
  }

  /**
   * Retrieve mount table object with all attributes derived from this object.
   *
   * @return MountTable object with updated attributes.
   * @throws IOException If mount table instantiation fails.
   */
  public MountTable getMountTableEntryWithAttributes() throws IOException {
    String normalizedMount = RouterAdmin.normalizeFileSystemPath(this.getMount());
    return getMountTableForAddRequest(normalizedMount);
  }

  /**
   * Create a new mount table object from the given mount point and update its attributes.
   *
   * @param mountSrc mount point src.
   * @return MountTable object with updated attributes.
   * @throws IOException If mount table instantiation fails.
   */
  private MountTable getMountTableForAddRequest(String mountSrc) throws IOException {
    Map<String, String> destMap = RouterAdmin.getDestMap(this.nss, this.destinations);
    MountTable newEntry = MountTable.newInstance(mountSrc, destMap);
    updateCommonAttributes(newEntry);
    return newEntry;
  }

  /**
   * Common attributes like read-only, fault-tolerant, dest order, owner, group, mode etc are
   * updated for the given mount table object.
   *
   * @param existingEntry Mount table object.
   */
  private void updateCommonAttributes(MountTable existingEntry) {
    if (this.isReadonly()) {
      existingEntry.setReadOnly(true);
    }
    if (this.isFaultTolerant()) {
      existingEntry.setFaultTolerant(true);
    }
    if (this.getOrder() != null) {
      existingEntry.setDestOrder(this.getOrder());
    }
    RouterAdmin.ACLEntity mountAclInfo = this.getAclInfo();
    // Update ACL info of mount table entry
    if (mountAclInfo.getOwner() != null) {
      existingEntry.setOwnerName(mountAclInfo.getOwner());
    }
    if (mountAclInfo.getGroup() != null) {
      existingEntry.setGroupName(mountAclInfo.getGroup());
    }
    if (mountAclInfo.getMode() != null) {
      existingEntry.setMode(mountAclInfo.getMode());
    }
    existingEntry.validate();
  }

}
