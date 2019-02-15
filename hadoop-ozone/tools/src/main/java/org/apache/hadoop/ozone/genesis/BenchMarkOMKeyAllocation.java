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

package org.apache.hadoop.ozone.genesis;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.BucketManager;
import org.apache.hadoop.ozone.om.BucketManagerImpl;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.KeyManagerImpl;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.VolumeManager;
import org.apache.hadoop.ozone.om.VolumeManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.security.UserGroupInformation;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Benchmark key creation in a bucket in OM.
 */
@State(Scope.Thread)
public class BenchMarkOMKeyAllocation {

  private static final String TMP_DIR = "java.io.tmpdir";
  private String volumeName = UUID.randomUUID().toString();
  private String bucketName = UUID.randomUUID().toString();
  private KeyManager keyManager;
  private VolumeManager volumeManager;
  private BucketManager bucketManager;
  private String path = Paths.get(System.getProperty(TMP_DIR)).resolve(
      RandomStringUtils.randomNumeric(6)).toFile()
            .getAbsolutePath();

  @Setup(Level.Trial)
  public void setup() throws IOException {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OMConfigKeys.OZONE_OM_DB_DIRS, path);

    OmMetadataManagerImpl omMetadataManager =
        new OmMetadataManagerImpl(configuration);
    volumeManager = new VolumeManagerImpl(omMetadataManager, configuration);
    bucketManager = new BucketManagerImpl(omMetadataManager);

    volumeManager.createVolume(new OmVolumeArgs.Builder().setVolume(volumeName)
        .setAdminName(UserGroupInformation.getLoginUser().getUserName())
        .setOwnerName(UserGroupInformation.getLoginUser().getUserName())
        .build());

    bucketManager.createBucket(new OmBucketInfo.Builder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName).build());

    keyManager = new KeyManagerImpl(null, omMetadataManager, configuration,
        UUID.randomUUID().toString(), null);
  }

  @TearDown(Level.Trial)
  public void cleanup() throws IOException {
    FileUtils.deleteDirectory(new File(path));
    keyManager.stop();
  }

  @Benchmark
  public void keyCreation() throws IOException {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OMConfigKeys.OZONE_OM_DB_DIRS, path);

    List<OmKeyLocationInfo> keyLocationInfos = getKeyInfoList();

    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(UUID.randomUUID().toString())
        .setDataSize(0)
        .setFactor(HddsProtos.ReplicationFactor.THREE)
        .setType(HddsProtos.ReplicationType.RATIS).build();
    OpenKeySession openKeySession = keyManager.openKey(omKeyArgs);
    // setting location info list
    omKeyArgs.setLocationInfoList(keyLocationInfos);
    keyManager.commitKey(omKeyArgs, openKeySession.getId());
  }

  public List<OmKeyLocationInfo> getKeyInfoList() {
    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();

    omKeyLocationInfoList.add(getKeyInfo());
    omKeyLocationInfoList.add(getKeyInfo());

    return omKeyLocationInfoList;
  }

  public OmKeyLocationInfo getKeyInfo() {
    return new OmKeyLocationInfo.Builder().setBlockID(
        new BlockID(RandomUtils.nextLong(0, 100000000),
            RandomUtils.nextLong(0, 10000000)))
        .setLength(RandomUtils.nextLong(0, 10000000))
        .setOffset(0).build();
  }
}
