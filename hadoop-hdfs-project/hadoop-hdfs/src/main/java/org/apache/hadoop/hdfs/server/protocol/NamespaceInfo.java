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

package org.apache.hadoop.hdfs.server.protocol;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.util.VersionInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * NamespaceInfo is returned by the name-node in reply 
 * to a data-node handshake.
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class NamespaceInfo extends StorageInfo {
  final String  buildVersion;
  String blockPoolID = "";    // id of the block pool
  String softwareVersion;
  long capabilities;

  // only authoritative on the server-side to determine advertisement to
  // clients.  enum will update the supported values
  private static final long CAPABILITIES_SUPPORTED = getSupportedCapabilities();

  private static long getSupportedCapabilities() {
    long mask = 0;
    for (Capability c : Capability.values()) {
      if (c.supported) {
        mask |= c.mask;
      }
    }
    return mask;
  }

  public enum Capability {
    UNKNOWN(false),
    STORAGE_BLOCK_REPORT_BUFFERS(true); // use optimized ByteString buffers
    private final boolean supported;
    private final long mask;
    Capability(boolean isSupported) {
      supported = isSupported;
      int bits = ordinal() - 1;
      mask = (bits < 0) ? 0 : (1L << bits);
    }
    public long getMask() {
      return mask;
    }
  }

  // defaults to enabled capabilites since this ctor is for server
  public NamespaceInfo() {
    super(NodeType.NAME_NODE);
    buildVersion = null;
    capabilities = CAPABILITIES_SUPPORTED;
  }

  // defaults to enabled capabilites since this ctor is for server
  public NamespaceInfo(int nsID, String clusterID, String bpID,
      long cT, String buildVersion, String softwareVersion) {
    this(nsID, clusterID, bpID, cT, buildVersion, softwareVersion,
        CAPABILITIES_SUPPORTED);
  }

  // for use by server and/or client
  public NamespaceInfo(int nsID, String clusterID, String bpID,
      long cT, String buildVersion, String softwareVersion,
      long capabilities) {
    super(HdfsConstants.NAMENODE_LAYOUT_VERSION, nsID, clusterID, cT,
        NodeType.NAME_NODE);
    blockPoolID = bpID;
    this.buildVersion = buildVersion;
    this.softwareVersion = softwareVersion;
    this.capabilities = capabilities;
  }

  public NamespaceInfo(int nsID, String clusterID, String bpID, 
      long cT) {
    this(nsID, clusterID, bpID, cT, Storage.getBuildVersion(),
        VersionInfo.getVersion());
  }
  
  public long getCapabilities() {
    return capabilities;
  }

  @VisibleForTesting
  public void setCapabilities(long capabilities) {
    this.capabilities = capabilities;
  }

  public boolean isCapabilitySupported(Capability capability) {
    Preconditions.checkArgument(capability != Capability.UNKNOWN,
        "cannot test for unknown capability");
    long mask = capability.getMask();
    return (capabilities & mask) == mask;
  }

  public String getBuildVersion() {
    return buildVersion;
  }

  public String getBlockPoolID() {
    return blockPoolID;
  }
  
  public String getSoftwareVersion() {
    return softwareVersion;
  }

  @Override
  public String toString(){
    return super.toString() + ";bpid=" + blockPoolID;
  }

  public void validateStorage(NNStorage storage) throws IOException {
    if (layoutVersion != storage.getLayoutVersion() ||
        namespaceID != storage.getNamespaceID() ||
        cTime != storage.cTime ||
        !clusterID.equals(storage.getClusterID()) ||
        !blockPoolID.equals(storage.getBlockPoolID())) {
      throw new IOException("Inconsistent namespace information:\n" +
          "NamespaceInfo has:\n" +
          "LV=" + layoutVersion + ";" +
          "NS=" + namespaceID + ";" +
          "cTime=" + cTime + ";" +
          "CID=" + clusterID + ";" +
          "BPID=" + blockPoolID +
          ".\nStorage has:\n" +
          "LV=" + storage.getLayoutVersion() + ";" +
          "NS=" + storage.getNamespaceID() + ";" +
          "cTime=" + storage.getCTime() + ";" +
          "CID=" + storage.getClusterID() + ";" +
          "BPID=" + storage.getBlockPoolID() + ".");
    }
  }
}
