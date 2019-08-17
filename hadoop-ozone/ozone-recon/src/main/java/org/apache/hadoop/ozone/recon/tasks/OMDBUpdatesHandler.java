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

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.VOLUME_TABLE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.utils.db.CodecRegistry;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used to listen on OM RocksDB updates.
 */
public class OMDBUpdatesHandler extends WriteBatch.Handler {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMDBUpdatesHandler.class);

  private Map<Integer, String> tablesNames;
  private CodecRegistry codecRegistry;
  private List<OMDBUpdateEvent> omdbUpdateEvents = new ArrayList<>();

  public OMDBUpdatesHandler(OMMetadataManager omMetadataManager) {
    tablesNames = omMetadataManager.getStore().getTableNames();
    codecRegistry = omMetadataManager.getStore().getCodecRegistry();
  }

  @Override
  public void put(int cfIndex, byte[] keyBytes, byte[] valueBytes) throws
      RocksDBException {
    try {
      processEvent(cfIndex, keyBytes, valueBytes,
          OMDBUpdateEvent.OMDBUpdateAction.PUT);
    } catch (IOException ioEx) {
      LOG.error("Exception when reading key : " + ioEx);
    }
  }

  @Override
  public void delete(int cfIndex, byte[] keyBytes) throws RocksDBException {
    try {
      processEvent(cfIndex, keyBytes, null,
          OMDBUpdateEvent.OMDBUpdateAction.DELETE);
    } catch (IOException ioEx) {
      LOG.error("Exception when reading key : " + ioEx);
    }
  }

  /**
   *
   */
  private void processEvent(int cfIndex, byte[] keyBytes, byte[]
      valueBytes, OMDBUpdateEvent.OMDBUpdateAction action)
      throws IOException {
    String tableName = tablesNames.get(cfIndex);
    Class keyType = getKeyType(tableName);
    Class valueType = getValueType(tableName);
    if (valueType != null) {
      OMDBUpdateEvent.OMUpdateEventBuilder builder =
          new OMDBUpdateEvent.OMUpdateEventBuilder<>();
      builder.setTable(tableName);

      Object key = codecRegistry.asObject(keyBytes, keyType);
      builder.setKey(key);

      if (!action.equals(OMDBUpdateEvent.OMDBUpdateAction.DELETE)) {
        Object value = codecRegistry.asObject(valueBytes, valueType);
        builder.setValue(value);
      }

      builder.setAction(action);
      OMDBUpdateEvent event = builder.build();
      LOG.info("Generated OM update Event for table : " + event.getTable()
          + ", Key = " + event.getKey());
      // Temporarily adding to an event buffer for testing. In subsequent JIRAs,
      // a Recon side class will be implemented that requests delta updates
      // from OM and calls on this handler. In that case, we will fill up
      // this buffer and pass it on to the ReconTaskController which has
      // tasks waiting on OM events.
      omdbUpdateEvents.add(event);
    }
  }

  // There are no use cases yet for the remaining methods in Recon. These
  // will be implemented as and when need arises.

  @Override
  public void put(byte[] bytes, byte[] bytes1) {

  }

  @Override
  public void merge(int i, byte[] bytes, byte[] bytes1)
      throws RocksDBException {
  }

  @Override
  public void merge(byte[] bytes, byte[] bytes1) {
  }

  @Override
  public void delete(byte[] bytes) {
  }

  @Override
  public void singleDelete(int i, byte[] bytes) throws RocksDBException {
  }

  @Override
  public void singleDelete(byte[] bytes) {
  }

  @Override
  public void deleteRange(int i, byte[] bytes, byte[] bytes1)
      throws RocksDBException {
  }

  @Override
  public void deleteRange(byte[] bytes, byte[] bytes1) {

  }

  @Override
  public void logData(byte[] bytes) {

  }

  @Override
  public void putBlobIndex(int i, byte[] bytes, byte[] bytes1)
      throws RocksDBException {
  }

  @Override
  public void markBeginPrepare() throws RocksDBException {

  }

  @Override
  public void markEndPrepare(byte[] bytes) throws RocksDBException {

  }

  @Override
  public void markNoop(boolean b) throws RocksDBException {

  }

  @Override
  public void markRollback(byte[] bytes) throws RocksDBException {

  }

  @Override
  public void markCommit(byte[] bytes) throws RocksDBException {

  }

  /**
   * Return Key type class for a given table name.
   * @param name table name.
   * @return String.class by default.
   */
  private Class getKeyType(String name) {
    return String.class;
  }

  /**
   * Return Value type class for a given table.
   * @param name table name
   * @return Value type based on table name.
   */
  @VisibleForTesting
  protected Class getValueType(String name) {
    switch (name) {
    case KEY_TABLE : return OmKeyInfo.class;
    case VOLUME_TABLE : return OmVolumeArgs.class;
    case BUCKET_TABLE : return OmBucketInfo.class;
    default: return null;
    }
  }

  /**
   * Get List of events. (Temporary API to unit test the class).
   * @return List of events.
   */
  public List<OMDBUpdateEvent> getEvents() {
    return omdbUpdateEvents;
  }

}
