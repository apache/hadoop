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
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.ObjectStoreHandler;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.helpers.KeyUtils;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyArgs;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyInfo;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.KeyArgs;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.utils.MetadataKeyFilters.MetadataKeyFilter;
import org.apache.hadoop.utils.MetadataStore;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.LinkedList;
import java.util.Set;

/**
 * A helper class used by {@link TestStorageContainerManager} to generate
 * some keys and helps to verify containers and blocks locations.
 */
public class TestStorageContainerManagerHelper {

  private final MiniOzoneClassicCluster cluster;
  private final Configuration conf;
  private final StorageHandler storageHandler;

  public TestStorageContainerManagerHelper(MiniOzoneClassicCluster cluster,
      Configuration conf) throws IOException {
    this.cluster = cluster;
    this.conf = conf;
    storageHandler = new ObjectStoreHandler(conf).getStorageHandler();
  }

  public Map<String, KsmKeyInfo> createKeys(int numOfKeys, int keySize)
      throws Exception {
    Map<String, KsmKeyInfo> keyLocationMap = Maps.newHashMap();
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
    bucketArgs.setAddAcls(new LinkedList<>());
    bucketArgs.setRemoveAcls(new LinkedList<>());
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
      KsmKeyArgs arg = new KsmKeyArgs.Builder()
          .setVolumeName(volume)
          .setBucketName(bucket)
          .setKeyName(key)
          .build();
      KsmKeyInfo location = cluster.getKeySpaceManager()
          .lookupKey(arg);
      keyLocationMap.put(key, location);
    }
    return keyLocationMap;
  }

  public List<String> getPendingDeletionBlocks(String containerName)
      throws IOException {
    List<String> pendingDeletionBlocks = Lists.newArrayList();
    MetadataStore meta = getContainerMetadata(containerName);
    KeyPrefixFilter filter =
        new KeyPrefixFilter(OzoneConsts.DELETING_KEY_PREFIX);
    List<Map.Entry<byte[], byte[]>> kvs = meta
        .getRangeKVs(null, Integer.MAX_VALUE, filter);
    kvs.forEach(entry -> {
      String key = DFSUtil.bytes2String(entry.getKey());
      pendingDeletionBlocks
          .add(key.replace(OzoneConsts.DELETING_KEY_PREFIX, ""));
    });
    return pendingDeletionBlocks;
  }

  public List<String> getAllBlocks(Set<String> containerNames)
      throws IOException {
    List<String> allBlocks = Lists.newArrayList();
    for (String containerName : containerNames) {
      allBlocks.addAll(getAllBlocks(containerName));
    }
    return allBlocks;
  }

  public List<String> getAllBlocks(String containerName) throws IOException {
    List<String> allBlocks = Lists.newArrayList();
    MetadataStore meta = getContainerMetadata(containerName);
    MetadataKeyFilter filter =
        (preKey, currentKey, nextKey) -> !DFSUtil.bytes2String(currentKey)
            .startsWith(OzoneConsts.DELETING_KEY_PREFIX);
    List<Map.Entry<byte[], byte[]>> kvs =
        meta.getRangeKVs(null, Integer.MAX_VALUE, filter);
    kvs.forEach(entry -> {
      String key = DFSUtil.bytes2String(entry.getKey());
      allBlocks.add(key.replace(OzoneConsts.DELETING_KEY_PREFIX, ""));
    });
    return allBlocks;
  }

  private MetadataStore getContainerMetadata(String containerName)
      throws IOException {
    Pipeline pipeline = cluster.getStorageContainerManager()
        .getContainer(containerName);
    DatanodeID leadDN = pipeline.getLeader();
    OzoneContainer containerServer =
        getContainerServerByDatanodeID(leadDN.getDatanodeUuid());
    ContainerData containerData = containerServer.getContainerManager()
        .readContainer(containerName);
    return KeyUtils.getDB(containerData, conf);
  }

  private OzoneContainer getContainerServerByDatanodeID(String dnUUID)
      throws IOException {
    for (DataNode dn : cluster.getDataNodes()) {
      if (dn.getDatanodeId().getDatanodeUuid().equals(dnUUID)) {
        return dn.getOzoneContainerManager();
      }
    }
    throw new IOException("Unable to get the ozone container "
        + "for given datanode ID " + dnUUID);
  }
}
