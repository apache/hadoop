/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.container.common.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.
    ContainerType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerDataProto;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import org.yaml.snakeyaml.Yaml;

import static org.apache.hadoop.ozone.OzoneConsts.CHECKSUM;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_ID;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_TYPE;
import static org.apache.hadoop.ozone.OzoneConsts.LAYOUTVERSION;
import static org.apache.hadoop.ozone.OzoneConsts.MAX_SIZE;
import static org.apache.hadoop.ozone.OzoneConsts.METADATA;
import static org.apache.hadoop.ozone.OzoneConsts.ORIGIN_NODE_ID;
import static org.apache.hadoop.ozone.OzoneConsts.ORIGIN_PIPELINE_ID;
import static org.apache.hadoop.ozone.OzoneConsts.STATE;

/**
 * ContainerData is the in-memory representation of container metadata and is
 * represented on disk by the .container file.
 */
public abstract class ContainerData {

  //Type of the container.
  // For now, we support only KeyValueContainer.
  private final ContainerType containerType;

  // Unique identifier for the container
  private final long containerID;

  // Layout version of the container data
  private final int layOutVersion;

  // Metadata of the container will be a key value pair.
  // This can hold information like volume name, owner etc.,
  private final Map<String, String> metadata;

  // State of the Container
  private ContainerDataProto.State state;

  private final long maxSize;

  //ID of the pipeline where this container is created
  private String originPipelineId;
  //ID of the datanode where this container is created
  private String originNodeId;

  /** parameters for read/write statistics on the container. **/
  private final AtomicLong readBytes;
  private final AtomicLong writeBytes;
  private final AtomicLong readCount;
  private final AtomicLong writeCount;
  private final AtomicLong bytesUsed;
  private final AtomicLong keyCount;

  private HddsVolume volume;

  private String checksum;
  public static final Charset CHARSET_ENCODING = Charset.forName("UTF-8");
  private static final String DUMMY_CHECKSUM = new String(new byte[64],
      CHARSET_ENCODING);

  // Common Fields need to be stored in .container file.
  protected static final List<String> YAML_FIELDS =
      Collections.unmodifiableList(Lists.newArrayList(
      CONTAINER_TYPE,
      CONTAINER_ID,
      LAYOUTVERSION,
      STATE,
      METADATA,
      MAX_SIZE,
      CHECKSUM,
      ORIGIN_PIPELINE_ID,
      ORIGIN_NODE_ID));

  /**
   * Creates a ContainerData Object, which holds metadata of the container.
   * @param type - ContainerType
   * @param containerId - ContainerId
   * @param size - container maximum size in bytes
   * @param originPipelineId - Pipeline Id where this container is/was created
   * @param originNodeId - Node Id where this container is/was created
   */
  protected ContainerData(ContainerType type, long containerId, long size,
                          String originPipelineId, String originNodeId) {
    this(type, containerId, ChunkLayOutVersion.getLatestVersion().getVersion(),
        size, originPipelineId, originNodeId);
  }

  /**
   * Creates a ContainerData Object, which holds metadata of the container.
   * @param type - ContainerType
   * @param containerId - ContainerId
   * @param layOutVersion - Container layOutVersion
   * @param size - Container maximum size in bytes
   * @param originPipelineId - Pipeline Id where this container is/was created
   * @param originNodeId - Node Id where this container is/was created
   */
  protected ContainerData(ContainerType type, long containerId,
      int layOutVersion, long size, String originPipelineId,
      String originNodeId) {
    Preconditions.checkNotNull(type);

    this.containerType = type;
    this.containerID = containerId;
    this.layOutVersion = layOutVersion;
    this.metadata = new TreeMap<>();
    this.state = ContainerDataProto.State.OPEN;
    this.readCount = new AtomicLong(0L);
    this.readBytes =  new AtomicLong(0L);
    this.writeCount =  new AtomicLong(0L);
    this.writeBytes =  new AtomicLong(0L);
    this.bytesUsed = new AtomicLong(0L);
    this.keyCount = new AtomicLong(0L);
    this.maxSize = size;
    this.originPipelineId = originPipelineId;
    this.originNodeId = originNodeId;
    setChecksumTo0ByteArray();
  }

  /**
   * Returns the containerID.
   */
  public long getContainerID() {
    return containerID;
  }

  /**
   * Returns the path to base dir of the container.
   * @return Path to base dir.
   */
  public abstract String getContainerPath();

  /**
   * Returns the type of the container.
   * @return ContainerType
   */
  public ContainerType getContainerType() {
    return containerType;
  }


  /**
   * Returns the state of the container.
   * @return ContainerLifeCycleState
   */
  public synchronized ContainerDataProto.State getState() {
    return state;
  }

  /**
   * Set the state of the container.
   * @param state
   */
  public synchronized void setState(ContainerDataProto.State state) {
    this.state = state;
  }

  /**
   * Return's maximum size of the container in bytes.
   * @return maxSize in bytes
   */
  public long getMaxSize() {
    return maxSize;
  }

  /**
   * Returns the layOutVersion of the actual container data format.
   * @return layOutVersion
   */
  public int getLayOutVersion() {
    return ChunkLayOutVersion.getChunkLayOutVersion(layOutVersion).getVersion();
  }

  /**
   * Add/Update metadata.
   * We should hold the container lock before updating the metadata as this
   * will be persisted on disk. Unless, we are reconstructing ContainerData
   * from protoBuf or from on disk .container file in which case lock is not
   * required.
   */
  public void addMetadata(String key, String value) {
    metadata.put(key, value);
  }

  /**
   * Retuns metadata of the container.
   * @return metadata
   */
  public Map<String, String> getMetadata() {
    return Collections.unmodifiableMap(this.metadata);
  }

  /**
   * Set metadata.
   * We should hold the container lock before updating the metadata as this
   * will be persisted on disk. Unless, we are reconstructing ContainerData
   * from protoBuf or from on disk .container file in which case lock is not
   * required.
   */
  public void setMetadata(Map<String, String> metadataMap) {
    metadata.clear();
    metadata.putAll(metadataMap);
  }

  /**
   * checks if the container is open.
   * @return - boolean
   */
  public synchronized  boolean isOpen() {
    return ContainerDataProto.State.OPEN == state;
  }

  /**
   * checks if the container is invalid.
   * @return - boolean
   */
  public synchronized boolean isValid() {
    return !(ContainerDataProto.State.INVALID == state);
  }

  /**
   * checks if the container is closed.
   * @return - boolean
   */
  public synchronized boolean isClosed() {
    return ContainerDataProto.State.CLOSED == state;
  }

  /**
   * checks if the container is quasi closed.
   * @return - boolean
   */
  public synchronized boolean isQuasiClosed() {
    return ContainerDataProto.State.QUASI_CLOSED == state;
  }

  /**
   * Marks this container as quasi closed.
   */
  public synchronized void quasiCloseContainer() {
    setState(ContainerDataProto.State.QUASI_CLOSED);
  }

  /**
   * Marks this container as closed.
   */
  public synchronized void closeContainer() {
    setState(ContainerDataProto.State.CLOSED);
  }

  /**
   * Get the number of bytes read from the container.
   * @return the number of bytes read from the container.
   */
  public long getReadBytes() {
    return readBytes.get();
  }

  /**
   * Increase the number of bytes read from the container.
   * @param bytes number of bytes read.
   */
  public void incrReadBytes(long bytes) {
    this.readBytes.addAndGet(bytes);
  }

  /**
   * Get the number of times the container is read.
   * @return the number of times the container is read.
   */
  public long getReadCount() {
    return readCount.get();
  }

  /**
   * Increase the number of container read count by 1.
   */
  public void incrReadCount() {
    this.readCount.incrementAndGet();
  }

  /**
   * Get the number of bytes write into the container.
   * @return the number of bytes write into the container.
   */
  public long getWriteBytes() {
    return writeBytes.get();
  }

  /**
   * Increase the number of bytes write into the container.
   * @param bytes the number of bytes write into the container.
   */
  public void incrWriteBytes(long bytes) {
    this.writeBytes.addAndGet(bytes);
  }

  /**
   * Get the number of writes into the container.
   * @return the number of writes into the container.
   */
  public long getWriteCount() {
    return writeCount.get();
  }

  /**
   * Increase the number of writes into the container by 1.
   */
  public void incrWriteCount() {
    this.writeCount.incrementAndGet();
  }

  /**
   * Sets the number of bytes used by the container.
   * @param used
   */
  public void setBytesUsed(long used) {
    this.bytesUsed.set(used);
  }

  /**
   * Get the number of bytes used by the container.
   * @return the number of bytes used by the container.
   */
  public long getBytesUsed() {
    return bytesUsed.get();
  }

  /**
   * Increase the number of bytes used by the container.
   * @param used number of bytes used by the container.
   * @return the current number of bytes used by the container afert increase.
   */
  public long incrBytesUsed(long used) {
    return this.bytesUsed.addAndGet(used);
  }

  /**
   * Decrease the number of bytes used by the container.
   * @param reclaimed the number of bytes reclaimed from the container.
   * @return the current number of bytes used by the container after decrease.
   */
  public long decrBytesUsed(long reclaimed) {
    return this.bytesUsed.addAndGet(-1L * reclaimed);
  }

  /**
   * Set the Volume for the Container.
   * This should be called only from the createContainer.
   * @param hddsVolume
   */
  public void setVolume(HddsVolume hddsVolume) {
    this.volume = hddsVolume;
  }

  /**
   * Returns the volume of the Container.
   * @return HddsVolume
   */
  public HddsVolume getVolume() {
    return volume;
  }

  /**
   * Increments the number of keys in the container.
   */
  public void incrKeyCount() {
    this.keyCount.incrementAndGet();
  }

  /**
   * Decrements number of keys in the container.
   */
  public void decrKeyCount() {
    this.keyCount.decrementAndGet();
  }

  /**
   * Returns number of keys in the container.
   * @return key count
   */
  public long getKeyCount() {
    return this.keyCount.get();
  }

  /**
   * Set's number of keys in the container.
   * @param count
   */
  public void setKeyCount(long count) {
    this.keyCount.set(count);
  }

  public void setChecksumTo0ByteArray() {
    this.checksum = DUMMY_CHECKSUM;
  }

  public void setChecksum(String checkSum) {
    this.checksum = checkSum;
  }

  public String getChecksum() {
    return this.checksum;
  }


  /**
   * Returns the origin pipeline Id of this container.
   * @return origin node Id
   */
  public String getOriginPipelineId() {
    return originPipelineId;
  }

  /**
   * Returns the origin node Id of this container.
   * @return origin node Id
   */
  public String getOriginNodeId() {
    return originNodeId;
  }

  /**
   * Compute the checksum for ContainerData using the specified Yaml (based
   * on ContainerType) and set the checksum.
   *
   * Checksum of ContainerData is calculated by setting the
   * {@link ContainerData#checksum} field to a 64-byte array with all 0's -
   * {@link ContainerData#DUMMY_CHECKSUM}. After the checksum is calculated,
   * the checksum field is updated with this value.
   *
   * @param yaml Yaml for ContainerType to get the ContainerData as Yaml String
   * @throws IOException
   */
  public void computeAndSetChecksum(Yaml yaml) throws IOException {
    // Set checksum to dummy value - 0 byte array, to calculate the checksum
    // of rest of the data.
    setChecksumTo0ByteArray();

    // Dump yaml data into a string to compute its checksum
    String containerDataYamlStr = yaml.dump(this);

    this.checksum = ContainerUtils.getChecksum(containerDataYamlStr);
  }

  /**
   * Returns a ProtoBuf Message from ContainerData.
   *
   * @return Protocol Buffer Message
   */
  public abstract ContainerProtos.ContainerDataProto getProtoBufMessage();
}
