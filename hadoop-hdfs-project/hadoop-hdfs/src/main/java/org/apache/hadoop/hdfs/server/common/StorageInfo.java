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
package org.apache.hadoop.hdfs.server.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.LayoutFeature;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.datanode.DataNodeLayoutVersion;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;

import com.google.common.base.Joiner;

/**
 * Common class for storage information.
 * 
 * TODO namespaceID should be long and computed as hash(address + port)
 */
@InterfaceAudience.Private
public class StorageInfo {
  public int   layoutVersion;   // layout version of the storage data
  public int   namespaceID;     // id of the file system
  public String clusterID;      // id of the cluster
  public long  cTime;           // creation time of the file system state

  protected final NodeType storageType; // Type of the node using this storage 
  
  protected static final String STORAGE_FILE_VERSION    = "VERSION";

  public StorageInfo(NodeType type) {
    this(0, 0, "", 0L, type);
  }

  public StorageInfo(int layoutV, int nsID, String cid, long cT, NodeType type) {
    layoutVersion = layoutV;
    clusterID = cid;
    namespaceID = nsID;
    cTime = cT;
    storageType = type;
  }
  
  public StorageInfo(StorageInfo from) {
    this(from.layoutVersion, from.namespaceID, from.clusterID, from.cTime,
        from.storageType);
  }

  /**
   * Layout version of the storage data.
   */
  public int    getLayoutVersion(){ return layoutVersion; }

  /**
   * Namespace id of the file system.<p>
   * Assigned to the file system at formatting and never changes after that.
   * Shared by all file system components.
   */
  public int    getNamespaceID()  { return namespaceID; }

  /**
   * cluster id of the file system.<p>
   */
  public String    getClusterID()  { return clusterID; }
  
  /**
   * Creation time of the file system state.<p>
   * Modified during upgrades.
   */
  public long   getCTime()        { return cTime; }
  
  public void   setStorageInfo(StorageInfo from) {
    layoutVersion = from.layoutVersion;
    clusterID = from.clusterID;
    namespaceID = from.namespaceID;
    cTime = from.cTime;
  }

  public boolean versionSupportsFederation(
      Map<Integer, SortedSet<LayoutFeature>> map) {
    return LayoutVersion.supports(map, LayoutVersion.Feature.FEDERATION,
        layoutVersion);
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("lv=").append(layoutVersion).append(";cid=").append(clusterID)
    .append(";nsid=").append(namespaceID).append(";c=").append(cTime);
    return sb.toString();
  }
  
  public String toColonSeparatedString() {
    return Joiner.on(":").join(
        layoutVersion, namespaceID, cTime, clusterID);
  }
  
  public static int getNsIdFromColonSeparatedString(String in) {
    return Integer.parseInt(in.split(":")[1]);
  }
  
  public static String getClusterIdFromColonSeparatedString(String in) {
    return in.split(":")[3];
  }
  
  /**
   * Read properties from the VERSION file in the given storage directory.
   */
  public void readProperties(StorageDirectory sd) throws IOException {
    Properties props = readPropertiesFile(sd.getVersionFile());
    setFieldsFromProperties(props, sd);
  }
  
  /**
   * Read properties from the the previous/VERSION file in the given storage directory.
   */
  public void readPreviousVersionProperties(StorageDirectory sd)
      throws IOException {
    Properties props = readPropertiesFile(sd.getPreviousVersionFile());
    setFieldsFromProperties(props, sd);
  }
  
  /**
   * Get common storage fields.
   * Should be overloaded if additional fields need to be get.
   * 
   * @param props properties
   * @throws IOException on error
   */
  protected void setFieldsFromProperties(
      Properties props, StorageDirectory sd) throws IOException {
    if (props == null) {
      return;
    }
    setLayoutVersion(props, sd);
    setNamespaceID(props, sd);
    setcTime(props, sd);
    setClusterId(props, layoutVersion, sd);
    checkStorageType(props, sd);
  }
  
  /** Validate and set storage type from {@link Properties}*/
  protected void checkStorageType(Properties props, StorageDirectory sd)
      throws InconsistentFSStateException {
    if (storageType == null) { //don't care about storage type
      return;
    }
    NodeType type = NodeType.valueOf(getProperty(props, sd, "storageType"));
    if (!storageType.equals(type)) {
      throw new InconsistentFSStateException(sd.root,
          "Incompatible node types: storageType=" + storageType
          + " but StorageDirectory type=" + type);
    }
  }
  
  /** Validate and set ctime from {@link Properties}*/
  protected void setcTime(Properties props, StorageDirectory sd)
      throws InconsistentFSStateException {
    cTime = Long.parseLong(getProperty(props, sd, "cTime"));
  }

  /** Validate and set clusterId from {@link Properties}*/
  protected void setClusterId(Properties props, int layoutVersion,
      StorageDirectory sd) throws InconsistentFSStateException {
    // Set cluster ID in version that supports federation
    if (LayoutVersion.supports(getServiceLayoutFeatureMap(),
        Feature.FEDERATION, layoutVersion)) {
      String cid = getProperty(props, sd, "clusterID");
      if (!(clusterID.equals("") || cid.equals("") || clusterID.equals(cid))) {
        throw new InconsistentFSStateException(sd.getRoot(),
            "cluster Id is incompatible with others.");
      }
      clusterID = cid;
    }
  }
  
  /** Validate and set layout version from {@link Properties}*/
  protected void setLayoutVersion(Properties props, StorageDirectory sd)
      throws IncorrectVersionException, InconsistentFSStateException {
    int lv = Integer.parseInt(getProperty(props, sd, "layoutVersion"));
    if (lv < getServiceLayoutVersion()) { // future version
      throw new IncorrectVersionException(getServiceLayoutVersion(), lv,
          "storage directory " + sd.root.getAbsolutePath());
    }
    layoutVersion = lv;
  }
  
  /** Validate and set namespaceID version from {@link Properties}*/
  protected void setNamespaceID(Properties props, StorageDirectory sd)
      throws InconsistentFSStateException {
    int nsId = Integer.parseInt(getProperty(props, sd, "namespaceID"));
    if (namespaceID != 0 && nsId != 0 && namespaceID != nsId) {
      throw new InconsistentFSStateException(sd.root,
          "namespaceID is incompatible with others.");
    }
    namespaceID = nsId;
  }

  public void setServiceLayoutVersion(int lv) {
    this.layoutVersion = lv;
  }

  public int getServiceLayoutVersion() {
    return storageType == NodeType.DATA_NODE ? HdfsServerConstants.DATANODE_LAYOUT_VERSION
        : HdfsServerConstants.NAMENODE_LAYOUT_VERSION;
  }

  public Map<Integer, SortedSet<LayoutFeature>> getServiceLayoutFeatureMap() {
    return storageType == NodeType.DATA_NODE? DataNodeLayoutVersion.FEATURES
        : NameNodeLayoutVersion.FEATURES;
  }
  
  protected static String getProperty(Properties props, StorageDirectory sd,
      String name) throws InconsistentFSStateException {
    String property = props.getProperty(name);
    if (property == null) {
      throw new InconsistentFSStateException(sd.root, "file "
          + STORAGE_FILE_VERSION + " has " + name + " missing.");
    }
    return property;
  }

  public static Properties readPropertiesFile(File from) throws IOException {
    if (from == null) {
      return null;
    }
    RandomAccessFile file = new RandomAccessFile(from, "rws");
    FileInputStream in = null;
    Properties props = new Properties();
    try {
      in = new FileInputStream(file.getFD());
      file.seek(0);
      props.load(in);
    } finally {
      if (in != null) {
        in.close();
      }
      file.close();
    }
    return props;
  }
}
