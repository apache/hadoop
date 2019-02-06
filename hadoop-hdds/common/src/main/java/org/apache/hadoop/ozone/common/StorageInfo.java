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
package org.apache.hadoop.ozone.common;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Properties;
import java.util.UUID;

/**
 * Common class for storage information. This class defines the common
 * properties and functions to set them , write them into the version file
 * and read them from the version file.
 *
 */
@InterfaceAudience.Private
public class StorageInfo {

  private Properties properties = new Properties();

  /**
   * Property to hold node type.
   */
  private static final String NODE_TYPE = "nodeType";
  /**
   * Property to hold ID of the cluster.
   */
  private static final String CLUSTER_ID = "clusterID";
  /**
   * Property to hold creation time of the storage.
   */
  private static final String CREATION_TIME = "cTime";

  /**
   * Constructs StorageInfo instance.
   * @param type
   *          Type of the node using the storage
   * @param cid
   *          Cluster ID
   * @param cT
   *          Cluster creation Time

   * @throws IOException - on Error.
   */
  public StorageInfo(NodeType type, String cid, long cT)
      throws IOException {
    Preconditions.checkNotNull(type);
    Preconditions.checkNotNull(cid);
    properties.setProperty(NODE_TYPE, type.name());
    properties.setProperty(CLUSTER_ID, cid);
    properties.setProperty(CREATION_TIME, String.valueOf(cT));
  }

  public StorageInfo(NodeType type, File propertiesFile)
      throws IOException {
    this.properties = readFrom(propertiesFile);
    verifyNodeType(type);
    verifyClusterId();
    verifyCreationTime();
  }

  public NodeType getNodeType() {
    return NodeType.valueOf(properties.getProperty(NODE_TYPE));
  }

  public String getClusterID() {
    return properties.getProperty(CLUSTER_ID);
  }

  public Long  getCreationTime() {
    String creationTime = properties.getProperty(CREATION_TIME);
    if(creationTime != null) {
      return Long.parseLong(creationTime);
    }
    return null;
  }

  public String getProperty(String key) {
    return properties.getProperty(key);
  }

  public void setProperty(String key, String value) {
    properties.setProperty(key, value);
  }

  public void setClusterId(String clusterId) {
    properties.setProperty(CLUSTER_ID, clusterId);
  }

  private void verifyNodeType(NodeType type)
      throws InconsistentStorageStateException {
    NodeType nodeType = getNodeType();
    Preconditions.checkNotNull(nodeType);
    if(type != nodeType) {
      throw new InconsistentStorageStateException("Expected NodeType: " + type +
          ", but found: " + nodeType);
    }
  }

  private void verifyClusterId()
      throws InconsistentStorageStateException {
    String clusterId = getClusterID();
    Preconditions.checkNotNull(clusterId);
    if(clusterId.isEmpty()) {
      throw new InconsistentStorageStateException("Cluster ID not found");
    }
  }

  private void verifyCreationTime() {
    Long creationTime = getCreationTime();
    Preconditions.checkNotNull(creationTime);
  }


  public void writeTo(File to)
      throws IOException {
    try (RandomAccessFile file = new RandomAccessFile(to, "rws");
         FileOutputStream out = new FileOutputStream(file.getFD())) {
      file.seek(0);
    /*
     * If server is interrupted before this line,
     * the version file will remain unchanged.
     */
      properties.store(out, null);
    /*
     * Now the new fields are flushed to the head of the file, but file
     * length can still be larger then required and therefore the file can
     * contain whole or corrupted fields from its old contents in the end.
     * If server is interrupted here and restarted later these extra fields
     * either should not effect server behavior or should be handled
     * by the server correctly.
     */
      file.setLength(out.getChannel().position());
    }
  }

  private Properties readFrom(File from) throws IOException {
    try (RandomAccessFile file = new RandomAccessFile(from, "rws");
        FileInputStream in = new FileInputStream(file.getFD())) {
      Properties props = new Properties();
      file.seek(0);
      props.load(in);
      return props;
    }
  }

  /**
   * Generate new clusterID.
   *
   * clusterID is a persistent attribute of the cluster.
   * It is generated when the cluster is created and remains the same
   * during the life cycle of the cluster.  When a new SCM node is initialized,
   * if this is a new cluster, a new clusterID is generated and stored.
   * @return new clusterID
   */
  public static String newClusterID() {
    return "CID-" + UUID.randomUUID().toString();
  }

}
