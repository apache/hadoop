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
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.ksm.exceptions.KSMException;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.VolumeList;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.VolumeInfo;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.utils.LevelDBStore;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.ozone.OzoneConsts.KSM_DB_NAME;
import static org.apache.hadoop.ozone.ksm
    .KSMConfigKeys.OZONE_KSM_DB_CACHE_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.ksm
    .KSMConfigKeys.OZONE_KSM_DB_CACHE_SIZE_MB;
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

  private final KeySpaceManager ksm;
  private final LevelDBStore store;
  private final ReadWriteLock lock;
  private final int maxUserVolumeCount;

  /**
   * Constructor.
   * @param conf - Ozone configuration.
   * @throws IOException
   */
  public VolumeManagerImpl(KeySpaceManager ksm, OzoneConfiguration conf)
      throws IOException {
    File metaDir = OzoneUtils.getScmMetadirPath(conf);
    final int cacheSize = conf.getInt(OZONE_KSM_DB_CACHE_SIZE_MB,
        OZONE_KSM_DB_CACHE_SIZE_DEFAULT);
    Options options = new Options();
    options.cacheSize(cacheSize * OzoneConsts.MB);
    File ksmDBFile = new File(metaDir.getPath(), KSM_DB_NAME);
    this.ksm = ksm;
    this.store = new LevelDBStore(ksmDBFile, options);
    lock = new ReentrantReadWriteLock();
    this.maxUserVolumeCount = conf.getInt(OZONE_KSM_USER_MAX_VOLUME,
        OZONE_KSM_USER_MAX_VOLUME_DEFAULT);
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() throws IOException {
    store.close();
  }

  /**
   * Creates a volume.
   * @param args - KsmVolumeArgs.
   */
  @Override
  public void createVolume(KsmVolumeArgs args) throws IOException {
    Preconditions.checkNotNull(args);
    lock.writeLock().lock();
    WriteBatch batch = store.createWriteBatch();
    try {
      byte[] volumeName = store.get(DFSUtil.string2Bytes(args.getVolume()));

      // Check of the volume already exists
      if(volumeName != null) {
        LOG.error("volume:{} already exists", args.getVolume());
        throw new KSMException(ResultCodes.FAILED_VOLUME_ALREADY_EXISTS);
      }

      // Next count the number of volumes for the user
      String dbUserName = "$" + args.getOwnerName();
      byte[] volumeList  = store.get(DFSUtil.string2Bytes(dbUserName));
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

      // Commit the volume information to leveldb
      VolumeInfo volumeInfo = args.getProtobuf();
      batch.put(DFSUtil.string2Bytes(args.getVolume()),
                                     volumeInfo.toByteArray());

      prevVolList.add(args.getVolume());
      VolumeList newVolList = VolumeList.newBuilder()
              .addAllVolumeNames(prevVolList).build();
      batch.put(DFSUtil.string2Bytes(dbUserName), newVolList.toByteArray());
      store.commitWriteBatch(batch);
      LOG.info("created volume:{} user:{}",
                                  args.getVolume(), args.getOwnerName());
    } catch (IOException | DBException ex) {
      ksm.getMetrics().incNumVolumeCreateFails();
      LOG.error("Volume creation failed for user:{} volname:{}",
                                args.getOwnerName(), args.getVolume(), ex);
      throw ex;
    } finally {
      store.closeWriteBatch(batch);
      lock.writeLock().unlock();
    }
  }
}
