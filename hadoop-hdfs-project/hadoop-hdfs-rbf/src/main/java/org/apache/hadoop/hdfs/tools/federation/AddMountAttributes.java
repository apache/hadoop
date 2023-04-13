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

import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;

/**
 * Add mount entry attributes to be used by Router admin.
 */
public class AddMountAttributes {

  private String mount;
  private String[] nss;
  private String dest;
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

  public String getDest() {
    return dest;
  }

  public void setDest(String dest) {
    this.dest = dest;
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
}
