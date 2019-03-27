/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.datanode.ObjectStoreHandler;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.KeyArgs;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.utils.MetadataKeyFilters;
import org.apache.hadoop.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.utils.MetadataStore;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A helper class used by {@link TestStorageContainerManager} to generate
 * some keys and helps to verify containers and blocks locations.
 */
public class TestStorageContainerManagerHelper {

  private final MiniOzoneCluster cluster;
  private final Configuration conf;
  private final StorageHandler storageHandler;

  public TestStorageContainerManagerHelper(MiniOzoneCluster cluster,
      Configuration conf) throws IOException {
    this.cluster = cluster;
    this.conf = conf;
    storageHandler = new ObjectStoreHandler(conf).getStorageHandler();
  }

  public Map<String, OmKeyInfo> createKeys(int numOfKeys, int keySize)
      throws Exception {
    Map<String, OmKeyInfo> keyLocationMap = Maps.newHashMap();
    String volume = "volume" + RandomStringUtils.randomNumeric(5);
    String bucket = "bucket" + RandomStringUtils.randomNumeric(5);
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    UserArgs userArgs = new UserArgs(null, OzoneUtils.getRequestID(),
        null, null, null, null);

    VolumeArgs createVolumeArgs = new VolumeArgs(volume, userArgs);
    createVolumeArgs.setUserName(userName);
    createVolumeArgs.setAdminName(adminName);
    storageHandler.createVolume(createVolumeArgs);

    BucketArgs bucketArgs = new BucketArgs(bucket, createVolumeArgs);
    bucketArgs.setAddAcls(new ArrayList<>());
    bucketArgs.setRemoveAcls(new ArrayList<>());
    bucketArgs.setStorageType(StorageType.DISK);
    storageHandler.createBucket(bucketArgs);

    // Write 20 keys in bucket.
    Set<String> keyNames = Sets.newHashSet();
    KeyArgs keyArgs;
    for (int i = 0; i < numOfKeys; i++) {
      String keyName = RandomStringUtils.randomAlphabetic(5) + i;
      keyNames.add(keyName);
      keyArgs = new KeyArgs(keyName, bucketArgs);
      keyArgs.setSize(keySize);
      // Just for testing list keys call, so no need to write real data.
      OutputStream stream = storageHandler.newKeyWriter(keyArgs);
      stream.write(DFSUtil.string2Bytes(
          RandomStringUtils.randomAlphabetic(5)));
      stream.close();
    }

    for (String key : keyNames) {
      OmKeyArgs arg = new OmKeyArgs.Builder()
          .setVolumeName(volume)
          .setBucketName(bucket)
          .setKeyName(key)
          .setRefreshPipeline(true)
          .build();
      OmKeyInfo location = cluster.getOzoneManager()
          .lookupKey(arg);
      keyLocationMap.put(key, location);
    }
    return keyLocationMap;
  }

  public List<String> getPendingDeletionBlocks(Long containerID)
      throws IOException {
    List<String> pendingDeletionBlocks = Lists.newArrayList();
    MetadataStore meta = getContainerMetadata(containerID);
    KeyPrefixFilter filter =
        new KeyPrefixFilter().addFilter(OzoneConsts.DELETING_KEY_PREFIX);
    List<Map.Entry<byte[], byte[]>> kvs = meta
        .getRangeKVs(null, Integer.MAX_VALUE, filter);
    kvs.forEach(entry -> {
      String key = DFSUtil.bytes2String(entry.getKey());
      pendingDeletionBlocks
          .add(key.replace(OzoneConsts.DELETING_KEY_PREFIX, ""));
    });
    return pendingDeletionBlocks;
  }

  public List<Long> getAllBlocks(Set<Long> containerIDs)
      throws IOException {
    List<Long> allBlocks = Lists.newArrayList();
    for (Long containerID : containerIDs) {
      allBlocks.addAll(getAllBlocks(containerID));
    }
    return allBlocks;
  }

  public List<Long> getAllBlocks(Long containeID) throws IOException {
    List<Long> allBlocks = Lists.newArrayList();
    MetadataStore meta = getContainerMetadata(containeID);
    List<Map.Entry<byte[], byte[]>> kvs =
        meta.getRangeKVs(null, Integer.MAX_VALUE,
            MetadataKeyFilters.getNormalKeyFilter());
    kvs.forEach(entry -> {
      allBlocks.add(Longs.fromByteArray(entry.getKey()));
    });
    return allBlocks;
  }

  private MetadataStore getContainerMetadata(Long containerID)
      throws IOException {
    ContainerWithPipeline containerWithPipeline = cluster
        .getStorageContainerManager().getClientProtocolServer()
        .getContainerWithPipeline(containerID);

    DatanodeDetails dn =
        containerWithPipeline.getPipeline().getFirstNode();
    OzoneContainer containerServer =
        getContainerServerByDatanodeUuid(dn.getUuidString());
    KeyValueContainerData containerData =
        (KeyValueContainerData) containerServer.getContainerSet()
        .getContainer(containerID).getContainerData();
    return BlockUtils.getDB(containerData, conf);
  }

  private OzoneContainer getContainerServerByDatanodeUuid(String dnUUID)
      throws IOException {
    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      if (dn.getDatanodeDetails().getUuidString().equals(dnUUID)) {
        return dn.getDatanodeStateMachine().getContainer();
      }
    }
    throw new IOException("Unable to get the ozone container "
        + "for given datanode ID " + dnUUID);
  }
}
