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

package org.apache.hadoop.ozone.recon.tasks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.utils.db.RDBStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDB;
import org.rocksdb.TransactionLogIterator;
import org.rocksdb.WriteBatch;

/**
 * Class used to test OMDBUpdatesHandler.
 */
public class TestOMDBUpdatesHandler {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OzoneConfiguration createNewTestPath() throws IOException {
    OzoneConfiguration configuration = new OzoneConfiguration();
    File newFolder = folder.newFolder();
    if (!newFolder.exists()) {
      assertTrue(newFolder.mkdirs());
    }
    ServerUtils.setOzoneMetaDirPath(configuration, newFolder.toString());
    return configuration;
  }

  @Test
  public void testPut() throws Exception {
    OzoneConfiguration configuration = createNewTestPath();
    OmMetadataManagerImpl metaMgr = new OmMetadataManagerImpl(configuration);

    String volumeKey = metaMgr.getVolumeKey("sampleVol");
    OmVolumeArgs args =
        OmVolumeArgs.newBuilder()
            .setVolume("sampleVol")
            .setAdminName("bilbo")
            .setOwnerName("bilbo")
            .build();
    metaMgr.getVolumeTable().put(volumeKey, args);

    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
        .setBucketName("bucketOne")
        .setVolumeName("sampleVol")
        .setKeyName("key_one")
        .setReplicationFactor(HddsProtos.ReplicationFactor.ONE)
        .setReplicationType(HddsProtos.ReplicationType.STAND_ALONE)
        .build();

    metaMgr.getKeyTable().put("/sampleVol/bucketOne/key_one", omKeyInfo);
    RDBStore rdbStore = (RDBStore) metaMgr.getStore();

    RocksDB rocksDB = rdbStore.getDb();
    TransactionLogIterator transactionLogIterator =
        rocksDB.getUpdatesSince(0);
    List<byte[]> writeBatches = new ArrayList<>();

    while(transactionLogIterator.isValid()) {
      TransactionLogIterator.BatchResult result =
          transactionLogIterator.getBatch();
      result.writeBatch().markWalTerminationPoint();
      WriteBatch writeBatch = result.writeBatch();
      writeBatches.add(writeBatch.data());
      transactionLogIterator.next();
    }

    OzoneConfiguration conf2 = createNewTestPath();
    OmMetadataManagerImpl reconOmmetaMgr = new OmMetadataManagerImpl(conf2);
    List<OMDBUpdateEvent> events = new ArrayList<>();
    for (byte[] data : writeBatches) {
      WriteBatch writeBatch = new WriteBatch(data);
      OMDBUpdatesHandler omdbUpdatesHandler =
          new OMDBUpdatesHandler(reconOmmetaMgr);
      writeBatch.iterate(omdbUpdatesHandler);
      events.addAll(omdbUpdatesHandler.getEvents());
    }
    assertNotNull(events);
    assertTrue(events.size() == 2);

    OMDBUpdateEvent volEvent = events.get(0);
    assertEquals(OMDBUpdateEvent.OMDBUpdateAction.PUT, volEvent.getAction());
    assertEquals(volumeKey, volEvent.getKey());
    assertEquals(args.getVolume(), ((OmVolumeArgs)volEvent.getValue())
        .getVolume());

    OMDBUpdateEvent keyEvent = events.get(1);
    assertEquals(OMDBUpdateEvent.OMDBUpdateAction.PUT, keyEvent.getAction());
    assertEquals("/sampleVol/bucketOne/key_one", keyEvent.getKey());
    assertEquals(omKeyInfo.getBucketName(),
        ((OmKeyInfo)keyEvent.getValue()).getBucketName());
  }

  @Test
  public void testDelete() throws Exception {
    OzoneConfiguration configuration = createNewTestPath();
    OmMetadataManagerImpl metaMgr = new OmMetadataManagerImpl(configuration);

    String volumeKey = metaMgr.getVolumeKey("sampleVol");
    OmVolumeArgs args =
        OmVolumeArgs.newBuilder()
            .setVolume("sampleVol")
            .setAdminName("bilbo")
            .setOwnerName("bilbo")
            .build();
    metaMgr.getVolumeTable().put(volumeKey, args);

    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
        .setBucketName("bucketOne")
        .setVolumeName("sampleVol")
        .setKeyName("key_one")
        .setReplicationFactor(HddsProtos.ReplicationFactor.ONE)
        .setReplicationType(HddsProtos.ReplicationType.STAND_ALONE)
        .build();

    metaMgr.getKeyTable().put("/sampleVol/bucketOne/key_one", omKeyInfo);

    metaMgr.getKeyTable().delete("/sampleVol/bucketOne/key_one");
    metaMgr.getVolumeTable().delete(volumeKey);

    RDBStore rdbStore = (RDBStore) metaMgr.getStore();

    RocksDB rocksDB = rdbStore.getDb();
    TransactionLogIterator transactionLogIterator =
        rocksDB.getUpdatesSince(0);
    List<byte[]> writeBatches = new ArrayList<>();

    while(transactionLogIterator.isValid()) {
      TransactionLogIterator.BatchResult result =
          transactionLogIterator.getBatch();
      result.writeBatch().markWalTerminationPoint();
      WriteBatch writeBatch = result.writeBatch();
      writeBatches.add(writeBatch.data());
      transactionLogIterator.next();
    }

    OzoneConfiguration conf2 = createNewTestPath();
    OmMetadataManagerImpl reconOmmetaMgr = new OmMetadataManagerImpl(conf2);
    List<OMDBUpdateEvent> events = new ArrayList<>();
    for (byte[] data : writeBatches) {
      WriteBatch writeBatch = new WriteBatch(data);
      OMDBUpdatesHandler omdbUpdatesHandler =
          new OMDBUpdatesHandler(reconOmmetaMgr);
      writeBatch.iterate(omdbUpdatesHandler);
      events.addAll(omdbUpdatesHandler.getEvents());
    }
    assertNotNull(events);
    assertTrue(events.size() == 4);

    OMDBUpdateEvent keyEvent = events.get(2);
    assertEquals(OMDBUpdateEvent.OMDBUpdateAction.DELETE, keyEvent.getAction());
    assertEquals("/sampleVol/bucketOne/key_one", keyEvent.getKey());

    OMDBUpdateEvent volEvent = events.get(3);
    assertEquals(OMDBUpdateEvent.OMDBUpdateAction.DELETE, volEvent.getAction());
    assertEquals(volumeKey, volEvent.getKey());
  }

  @Test
  public void testGetValueType() throws IOException {
    OzoneConfiguration configuration = createNewTestPath();
    OmMetadataManagerImpl metaMgr = new OmMetadataManagerImpl(configuration);
    OMDBUpdatesHandler omdbUpdatesHandler =
        new OMDBUpdatesHandler(metaMgr);

    assertEquals(OmKeyInfo.class, omdbUpdatesHandler.getValueType(
        metaMgr.getKeyTable().getName()));
    assertEquals(OmVolumeArgs.class, omdbUpdatesHandler.getValueType(
        metaMgr.getVolumeTable().getName()));
    assertEquals(OmBucketInfo.class, omdbUpdatesHandler.getValueType(
        metaMgr.getBucketTable().getName()));
  }
}