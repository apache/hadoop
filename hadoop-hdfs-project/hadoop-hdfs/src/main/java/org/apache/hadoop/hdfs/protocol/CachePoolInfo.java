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

package org.apache.hadoop.hdfs.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.annotation.Nullable;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.util.XMLUtils;
import org.apache.hadoop.hdfs.util.XMLUtils.InvalidXmlException;
import org.apache.hadoop.hdfs.util.XMLUtils.Stanza;
import org.apache.hadoop.io.Text;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * CachePoolInfo describes a cache pool.
 *
 * This class is used in RPCs to create and modify cache pools.
 * It is serializable and can be stored in the edit log.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CachePoolInfo {
  public static final Log LOG = LogFactory.getLog(CachePoolInfo.class);

  final String poolName;

  @Nullable
  String ownerName;

  @Nullable
  String groupName;

  @Nullable
  FsPermission mode;

  @Nullable
  Integer weight;

  public CachePoolInfo(String poolName) {
    this.poolName = poolName;
  }
  
  public String getPoolName() {
    return poolName;
  }

  public String getOwnerName() {
    return ownerName;
  }

  public CachePoolInfo setOwnerName(String ownerName) {
    this.ownerName = ownerName;
    return this;
  }

  public String getGroupName() {
    return groupName;
  }

  public CachePoolInfo setGroupName(String groupName) {
    this.groupName = groupName;
    return this;
  }
  
  public FsPermission getMode() {
    return mode;
  }

  public CachePoolInfo setMode(FsPermission mode) {
    this.mode = mode;
    return this;
  }

  public Integer getWeight() {
    return weight;
  }

  public CachePoolInfo setWeight(Integer weight) {
    this.weight = weight;
    return this;
  }

  public String toString() {
    return new StringBuilder().append("{").
      append("poolName:").append(poolName).
      append(", ownerName:").append(ownerName).
      append(", groupName:").append(groupName).
      append(", mode:").append((mode == null) ? "null" :
          String.format("0%03o", mode.toShort())).
      append(", weight:").append(weight).
      append("}").toString();
  }
  
  @Override
  public boolean equals(Object o) {
    if (o == null) { return false; }
    if (o == this) { return true; }
    if (o.getClass() != getClass()) {
      return false;
    }
    CachePoolInfo other = (CachePoolInfo)o;
    return new EqualsBuilder().
        append(poolName, other.poolName).
        append(ownerName, other.ownerName).
        append(groupName, other.groupName).
        append(mode, other.mode).
        append(weight, other.weight).
        isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().
        append(poolName).
        append(ownerName).
        append(groupName).
        append(mode).
        append(weight).
        hashCode();
  }

  public static void validate(CachePoolInfo info) throws IOException {
    if (info == null) {
      throw new IOException("CachePoolInfo is null");
    }
    validateName(info.poolName);
  }

  public static void validateName(String poolName) throws IOException {
    if (poolName == null || poolName.isEmpty()) {
      // Empty pool names are not allowed because they would be highly
      // confusing.  They would also break the ability to list all pools
      // by starting with prevKey = ""
      throw new IOException("invalid empty cache pool name");
    }
  }

  public static CachePoolInfo readFrom(DataInput in) throws IOException {
    String poolName = Text.readString(in);
    CachePoolInfo info = new CachePoolInfo(poolName);
    if (in.readBoolean()) {
      info.setOwnerName(Text.readString(in));
    }
    if (in.readBoolean())  {
      info.setGroupName(Text.readString(in));
    }
    if (in.readBoolean()) {
      info.setMode(FsPermission.read(in));
    }
    if (in.readBoolean()) {
      info.setWeight(in.readInt());
    }
    return info;
  }

  public void writeTo(DataOutput out) throws IOException {
    Text.writeString(out, poolName);
    boolean hasOwner, hasGroup, hasMode, hasWeight;
    hasOwner = ownerName != null;
    hasGroup = groupName != null;
    hasMode = mode != null;
    hasWeight = weight != null;
    out.writeBoolean(hasOwner);
    if (hasOwner) {
      Text.writeString(out, ownerName);
    }
    out.writeBoolean(hasGroup);
    if (hasGroup) {
      Text.writeString(out, groupName);
    }
    out.writeBoolean(hasMode);
    if (hasMode) {
      mode.write(out);
    }
    out.writeBoolean(hasWeight);
    if (hasWeight) {
      out.writeInt(weight);
    }
  }

  public void writeXmlTo(ContentHandler contentHandler) throws SAXException {
    XMLUtils.addSaxString(contentHandler, "POOLNAME", poolName);
    PermissionStatus perm = new PermissionStatus(ownerName,
        groupName, mode);
    FSEditLogOp.permissionStatusToXml(contentHandler, perm);
    XMLUtils.addSaxString(contentHandler, "WEIGHT", Integer.toString(weight));
  }

  public static CachePoolInfo readXmlFrom(Stanza st) throws InvalidXmlException {
    String poolName = st.getValue("POOLNAME");
    PermissionStatus perm = FSEditLogOp.permissionStatusFromXml(st);
    int weight = Integer.parseInt(st.getValue("WEIGHT"));
    return new CachePoolInfo(poolName).
        setOwnerName(perm.getUserName()).
        setGroupName(perm.getGroupName()).
        setMode(perm.getPermission()).
        setWeight(weight);
  }
}