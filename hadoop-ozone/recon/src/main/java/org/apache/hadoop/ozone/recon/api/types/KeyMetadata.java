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
package org.apache.hadoop.ozone.recon.api.types;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

/**
 * Metadata object represents one key in the object store.
 */
@XmlRootElement (name = "KeyMetadata")
@XmlAccessorType(XmlAccessType.FIELD)
public class KeyMetadata {

  @XmlElement(name = "Volume")
  private String volume;

  @XmlElement(name = "Bucket")
  private String bucket;

  @XmlElement(name = "Key")
  private String key;

  @XmlElement(name = "DataSize")
  private long dataSize;

  @XmlElement(name = "Versions")
  private List<Long> versions;

  @XmlElement(name = "Blocks")
  private Map<Long, List<ContainerBlockMetadata>> blockIds;

  @XmlJavaTypeAdapter(IsoDateAdapter.class)
  @XmlElement(name = "CreationTime")
  private Instant creationTime;

  @XmlJavaTypeAdapter(IsoDateAdapter.class)
  @XmlElement(name = "ModificationTime")
  private Instant modificationTime;

  public String getVolume() {
    return volume;
  }

  public void setVolume(String volume) {
    this.volume = volume;
  }

  public String getBucket() {
    return bucket;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public long getDataSize() {
    return dataSize;
  }

  public void setDataSize(long dataSize) {
    this.dataSize = dataSize;
  }

  public Instant getCreationTime() {
    return creationTime;
  }

  public void setCreationTime(Instant creationTime) {
    this.creationTime = creationTime;
  }

  public Instant getModificationTime() {
    return modificationTime;
  }

  public void setModificationTime(Instant modificationTime) {
    this.modificationTime = modificationTime;
  }

  public List<Long> getVersions() {
    return versions;
  }

  public void setVersions(List<Long> versions) {
    this.versions = versions;
  }

  public Map<Long, List<ContainerBlockMetadata>> getBlockIds() {
    return blockIds;
  }

  public void setBlockIds(Map<Long, List<ContainerBlockMetadata>> blockIds) {
    this.blockIds = blockIds;
  }

  /**
   * Class to hold ContainerID and BlockID.
   */
  public static class ContainerBlockMetadata {
    private long containerID;
    private long localID;

    public ContainerBlockMetadata(long containerID, long localID) {
      this.containerID = containerID;
      this.localID = localID;
    }

    public long getContainerID() {
      return containerID;
    }

    public long getLocalID() {
      return localID;
    }
  }
}
