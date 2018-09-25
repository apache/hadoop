/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.hadoop.ozone.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * ObjectStore implementation with in-memory state.
 */
public class ObjectStoreStub extends ObjectStore {

  public ObjectStoreStub() {
    super();
  }

  private Map<String, OzoneVolumeStub> volumes = new HashMap<>();

  @Override
  public void createVolume(String volumeName) throws IOException {
    createVolume(volumeName,
        VolumeArgs.newBuilder()
            .setAdmin("root")
            .setOwner("root")
            .setQuota("" + Integer.MAX_VALUE)
            .setAcls(new ArrayList<>()).build());
  }

  @Override
  public void createVolume(String volumeName, VolumeArgs volumeArgs)
      throws IOException {
    OzoneVolumeStub volume =
        new OzoneVolumeStub(volumeName,
            volumeArgs.getAdmin(),
            volumeArgs.getOwner(),
            Long.parseLong(volumeArgs.getQuota()),
            System.currentTimeMillis(),
            volumeArgs.getAcls());
    volumes.put(volumeName, volume);
  }

  @Override
  public OzoneVolume getVolume(String volumeName) throws IOException {
    if (volumes.containsKey(volumeName)) {
      return volumes.get(volumeName);
    } else {
      throw new IOException("VOLUME_NOT_FOUND");
    }
  }

  @Override
  public Iterator<? extends OzoneVolume> listVolumes(String volumePrefix)
      throws IOException {
    return volumes.values()
        .stream()
        .filter(volume -> volume.getName().startsWith(volumePrefix))
        .collect(Collectors.toList())
        .iterator();

  }

  @Override
  public Iterator<? extends OzoneVolume> listVolumes(String volumePrefix,
      String prevVolume) throws IOException {
    return volumes.values()
        .stream()
        .filter(volume -> volume.getName().compareTo(prevVolume) > 0)
        .filter(volume -> volume.getName().startsWith(volumePrefix))
        .collect(Collectors.toList())
        .iterator();
  }

  @Override
  public Iterator<? extends OzoneVolume> listVolumesByUser(String user,
      String volumePrefix, String prevVolume) throws IOException {
    return volumes.values()
        .stream()
        .filter(volume -> volume.getOwner().equals(user))
        .filter(volume -> volume.getName().compareTo(prevVolume) < 0)
        .filter(volume -> volume.getName().startsWith(volumePrefix))
        .collect(Collectors.toList())
        .iterator();
  }

  @Override
  public void deleteVolume(String volumeName) throws IOException {
    volumes.remove(volumeName);
  }
}
