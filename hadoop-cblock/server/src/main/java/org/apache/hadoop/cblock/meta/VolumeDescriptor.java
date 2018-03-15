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
package org.apache.hadoop.cblock.meta;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.cblock.protocol.proto.CBlockClientServerProtocolProtos;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The internal representation maintained by CBlock server as the info for
 * a volume. Contains the list of containers belonging to this volume.
 *
 * Many methods of this class is made such that the volume information (
 * including container list) can be easily transformed into a Json string
 * that can be stored/parsed from a persistent store for cblock server
 * persistence.
 *
 * This class is still work-in-progress.
 */
public class VolumeDescriptor {
  // The main data structure is the container location map
  // other thing are mainly just information

  // since only one operation at a time is allowed, no
  // need to consider concurrency control here

  // key is container id

  private static final Logger LOG =
      LoggerFactory.getLogger(VolumeDescriptor.class);

  private ConcurrentHashMap<String, ContainerDescriptor> containerMap;
  private String userName;
  private int blockSize;
  private long volumeSize;
  private String volumeName;
  // this is essentially the ordered keys of containerMap
  // which is kind of redundant information. But since we
  // are likely to access it frequently based on ordering.
  // keeping this copy to avoid having to sort the key every
  // time
  private List<String> containerIdOrdered;

  /**
   * This is not being called explicitly, but this is necessary as
   * it will be called by the parse method implicitly when
   * reconstructing the object from json string. The get*() methods
   * and set*() methods are for the same purpose also.
   */
  public VolumeDescriptor() {
    this(null, null, 0, 0);
  }

  public VolumeDescriptor(String userName, String volumeName, long volumeSize,
      int blockSize) {
    this.containerMap = new ConcurrentHashMap<>();
    this.userName = userName;
    this.volumeName = volumeName;
    this.blockSize = blockSize;
    this.volumeSize = volumeSize;
    this.containerIdOrdered = new LinkedList<>();
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public void setVolumeName(String volumeName) {
    this.volumeName = volumeName;
  }

  public long getVolumeSize() {
    return volumeSize;
  }

  public void setVolumeSize(long volumeSize) {
    this.volumeSize = volumeSize;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public void setBlockSize(int blockSize) {
    this.blockSize = blockSize;
  }

  public void setContainerIDs(ArrayList<String> containerIDs) {
    containerIdOrdered.addAll(containerIDs);
  }

  public void addContainer(ContainerDescriptor containerDescriptor) {
    containerMap.put(containerDescriptor.getContainerID(),
        containerDescriptor);
  }


  public HashMap<String, Pipeline> getPipelines() {
    HashMap<String, Pipeline> pipelines = new HashMap<>();
    for (Map.Entry<String, ContainerDescriptor> entry :
        containerMap.entrySet()) {
      pipelines.put(entry.getKey(), entry.getValue().getPipeline());
    }
    return pipelines;
  }

  public boolean isEmpty() {
    VolumeInfo info = getInfo();
    return info.getUsage() == 0;
  }

  public VolumeInfo getInfo() {
    // TODO : need to actually go through all containers of this volume and
    // ask for their utilization.
    long utilization = 0;
    for (Map.Entry<String, ContainerDescriptor> entry :
        containerMap.entrySet()) {
      utilization += entry.getValue().getUtilization();
    }
    return new VolumeInfo(this.userName, this.volumeName,
        this.volumeSize, this.blockSize,
        utilization * blockSize);
  }

  public String[] getContainerIDs() {
    //ArrayList<Long> ids = new ArrayList(containerMap.keySet());
    //return ids.toArray(new Long[ids.size()]);
    return containerIdOrdered.toArray(new String[containerIdOrdered.size()]);
  }

  public List<String> getContainerIDsList() {
    return new ArrayList<>(containerIdOrdered);
  }

  public List<Pipeline> getContainerPipelines() {
    Map<String, Pipeline> tmp = getPipelines();
    List<Pipeline> pipelineList = new LinkedList<>();
    for (String containerIDString : containerIdOrdered) {
      pipelineList.add(tmp.get(containerIDString));
    }
    return pipelineList;
  }

  @Override
  public String toString() {
    String string = "";
    string += "Username:" + userName + "\n";
    string += "VolumeName:" + volumeName + "\n";
    string += "VolumeSize:" + volumeSize + "\n";
    string += "blockSize:" + blockSize + "\n";
    string += "containerIds:" + containerIdOrdered + "\n";
    string += "containerIdsWithObject:" + containerMap.keySet();
    return string;
  }

  public CBlockClientServerProtocolProtos.MountVolumeResponseProto
      toProtobuf() {
    CBlockClientServerProtocolProtos.MountVolumeResponseProto.Builder volume =
        CBlockClientServerProtocolProtos.MountVolumeResponseProto.newBuilder();
    volume.setIsValid(true);
    volume.setVolumeName(volumeName);
    volume.setUserName(userName);
    volume.setVolumeSize(volumeSize);
    volume.setBlockSize(blockSize);
    for (String containerIDString : containerIdOrdered) {
      ContainerDescriptor containerDescriptor = containerMap.get(
          containerIDString);
      volume.addAllContainerIDs(containerDescriptor.toProtobuf());
    }
    return volume.build();
  }

  public static VolumeDescriptor fromProtobuf(byte[] data)
      throws InvalidProtocolBufferException {
    CBlockClientServerProtocolProtos.MountVolumeResponseProto volume =
        CBlockClientServerProtocolProtos.MountVolumeResponseProto
            .parseFrom(data);
    String userName = volume.getUserName();
    String volumeName = volume.getVolumeName();
    long volumeSize = volume.getVolumeSize();
    int blockSize = volume.getBlockSize();
    VolumeDescriptor volumeDescriptor = new VolumeDescriptor(userName,
        volumeName, volumeSize, blockSize);
    List<CBlockClientServerProtocolProtos.ContainerIDProto> containers
        = volume.getAllContainerIDsList();

    String[] containerOrdering = new String[containers.size()];

    for (CBlockClientServerProtocolProtos.ContainerIDProto containerProto :
        containers) {
      ContainerDescriptor containerDescriptor = new ContainerDescriptor(
          containerProto.getContainerID(),
          (int)containerProto.getIndex());
      if(containerProto.hasPipeline()) {
        containerDescriptor.setPipeline(
            Pipeline.getFromProtoBuf(containerProto.getPipeline()));
      }
      volumeDescriptor.addContainer(containerDescriptor);
      containerOrdering[containerDescriptor.getContainerIndex()] =
          containerDescriptor.getContainerID();
    }
    volumeDescriptor.setContainerIDs(
        new ArrayList<>(Arrays.asList(containerOrdering)));
    return volumeDescriptor;
  }

  @Override
  public int hashCode() {
    return userName.hashCode()*37 + volumeName.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o != null && o instanceof VolumeDescriptor) {
      VolumeDescriptor other = (VolumeDescriptor)o;
      if (!userName.equals(other.getUserName()) ||
          !volumeName.equals(other.getVolumeName()) ||
          volumeSize != other.getVolumeSize() ||
          blockSize != other.getBlockSize()) {
        return false;
      }
      if (containerIdOrdered.size() != other.containerIdOrdered.size() ||
          containerMap.size() != other.containerMap.size()) {
        return false;
      }
      for (int i = 0; i<containerIdOrdered.size(); i++) {
        if (!containerIdOrdered.get(i).equals(
            other.containerIdOrdered.get(i))) {
          return false;
        }
      }
      return containerMap.equals(other.containerMap);
    }
    return false;
  }
}
