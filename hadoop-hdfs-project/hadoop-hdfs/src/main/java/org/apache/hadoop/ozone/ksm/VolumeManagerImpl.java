/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.ksm;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ksm.helpers.KsmVolumeArgs;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.ksm.exceptions.KSMException;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.VolumeList;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.VolumeInfo;
import org.iq80.leveldb.DBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.ksm
    .KSMConfigKeys.OZONE_KSM_USER_MAX_VOLUME_DEFAULT;
import static org.apache.hadoop.ozone.ksm
    .KSMConfigKeys.OZONE_KSM_USER_MAX_VOLUME;
import static org.apache.hadoop.ozone.ksm.exceptions
    .KSMException.ResultCodes;

/**
 * KSM volume management code.
 */
public class VolumeManagerImpl implements VolumeManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(VolumeManagerImpl.class);

  private final MetadataManager metadataManager;
  private final int maxUserVolumeCount;

  /**
   * Constructor.
   * @param conf - Ozone configuration.
   * @throws IOException
   */
  public VolumeManagerImpl(MetadataManager metadataManager,
      OzoneConfiguration conf) throws IOException {
    this.metadataManager = metadataManager;
    this.maxUserVolumeCount = conf.getInt(OZONE_KSM_USER_MAX_VOLUME,
        OZONE_KSM_USER_MAX_VOLUME_DEFAULT);
  }

  /**
   * Creates a volume.
   * @param args - KsmVolumeArgs.
   */
  @Override
  public void createVolume(KsmVolumeArgs args) throws IOException {
    Preconditions.checkNotNull(args);
    metadataManager.writeLock().lock();
    List<Map.Entry<byte[], byte[]>> batch = new LinkedList<>();
    try {
      byte[] volumeName = metadataManager.
          get(DFSUtil.string2Bytes(args.getVolume()));

      // Check of the volume already exists
      if(volumeName != null) {
        LOG.error("volume:{} already exists", args.getVolume());
        throw new KSMException(ResultCodes.FAILED_VOLUME_ALREADY_EXISTS);
      }

      // Next count the number of volumes for the user
      String dbUserName = "$" + args.getOwnerName();
      byte[] volumeList  = metadataManager
          .get(DFSUtil.string2Bytes(dbUserName));
      List prevVolList;
      if (volumeList != null) {
        VolumeList vlist = VolumeList.parseFrom(volumeList);
        prevVolList = vlist.getVolumeNamesList();
      } else {
        prevVolList = new LinkedList();
      }

      if (prevVolList.size() >= maxUserVolumeCount) {
        LOG.error("Too many volumes for user:{}", args.getOwnerName());
        throw new KSMException(ResultCodes.FAILED_TOO_MANY_USER_VOLUMES);
      }

      // Commit the volume information to metadataManager
      VolumeInfo volumeInfo = args.getProtobuf();
      batch.add(new AbstractMap.SimpleEntry<>(
          DFSUtil.string2Bytes(args.getVolume()), volumeInfo.toByteArray()));

      prevVolList.add(args.getVolume());
      VolumeList newVolList = VolumeList.newBuilder()
              .addAllVolumeNames(prevVolList).build();
      batch.add(new AbstractMap.SimpleEntry<>(
          DFSUtil.string2Bytes(dbUserName), newVolList.toByteArray()));
      metadataManager.batchPut(batch);
      LOG.info("created volume:{} user:{}",
                                  args.getVolume(), args.getOwnerName());
    } catch (IOException | DBException ex) {
      LOG.error("Volume creation failed for user:{} volname:{}",
                                args.getOwnerName(), args.getVolume(), ex);
      throw ex;
    } finally {
      metadataManager.writeLock().unlock();
    }
  }
}
