/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.ozone.genesis;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.*;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.protocol.ClientId;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ENABLED;

/**
 * Benchmarks OM Client.
 */
@State(Scope.Thread)
public class BenchMarkOMClient {

  private static String testDir;
  private static ReentrantLock lock = new ReentrantLock();
  private static String volumeName = UUID.randomUUID().toString();
  private static String bucketName = UUID.randomUUID().toString();
  private static List<String> keyNames = new ArrayList<>();
  private static List<Long> clientIDs = new ArrayList<>();
  private static OzoneManagerProtocolClientSideTranslatorPB ozoneManagerClient;
  private static volatile boolean bool = false;

  @Setup(Level.Trial)
  public static void initialize() throws IOException {
    try {
      lock.lock();
      if (!bool) {
        bool = true;
        OzoneConfiguration conf = new OzoneConfiguration();
        conf.setBoolean(OZONE_ENABLED, true);
        testDir = GenesisUtil.getTempPath()
            .resolve(RandomStringUtils.randomNumeric(7)).toString();
        conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir);

        // set the ip address and port number for the OM service
        conf.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, "OMADDR:PORT");
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        long omVersion = RPC.getProtocolVersion(OzoneManagerProtocolPB.class);
        InetSocketAddress omAddress = OmUtils.getOmAddressForClients(conf);
        RPC.setProtocolEngine(conf, OzoneManagerProtocolPB.class,
            ProtobufRpcEngine.class);
        ozoneManagerClient = new OzoneManagerProtocolClientSideTranslatorPB(
            RPC.getProxy(OzoneManagerProtocolPB.class, omVersion, omAddress,
                ugi, conf, NetUtils.getDefaultSocketFactory(conf),
                Client.getRpcTimeout(conf)), ClientId.randomId().toString());

        // prepare OM
        ozoneManagerClient.createVolume(
            new OmVolumeArgs.Builder().setVolume(volumeName)
                .setAdminName(UserGroupInformation.getLoginUser().getUserName())
                .setOwnerName(UserGroupInformation.getLoginUser().getUserName())
                .build());
        ozoneManagerClient.createBucket(
            new OmBucketInfo.Builder().setBucketName(bucketName)
                .setVolumeName(volumeName).build());
        createKeys(10);
      }
    } finally {
      lock.unlock();
    }
  }

  private static void createKeys(int numKeys) throws IOException {
    for (int i = 0; i < numKeys; i++) {
      String key = UUID.randomUUID().toString();
      OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .setKeyName(key)
          .setDataSize(0)
          .setFactor(HddsProtos.ReplicationFactor.ONE)
          .setType(HddsProtos.ReplicationType.RATIS)
          .build();
      OpenKeySession keySession = ozoneManagerClient.openKey(omKeyArgs);
      long clientID = keySession.getId();
      keyNames.add(key);
      clientIDs.add(clientID);
    }
  }

  @TearDown(Level.Trial)
  public static void tearDown() throws IOException {
    try {
      lock.lock();
      if (ozoneManagerClient != null) {
        ozoneManagerClient.close();
        ozoneManagerClient = null;
        FileUtil.fullyDelete(new File(testDir));
      }
    } finally {
      lock.unlock();
    }
  }

  @Threads(6)
  @Benchmark
  public void allocateBlockBenchMark(BenchMarkOMClient state,
      Blackhole bh) throws IOException {
    int index = (int) (Math.random() * keyNames.size());
    String key = keyNames.get(index);
    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(key)
        .setDataSize(50)
        .setFactor(HddsProtos.ReplicationFactor.ONE)
        .setType(HddsProtos.ReplicationType.RATIS)
        .build();
    state.ozoneManagerClient
        .allocateBlock(omKeyArgs, clientIDs.get(index), new ExcludeList());
  }
}
