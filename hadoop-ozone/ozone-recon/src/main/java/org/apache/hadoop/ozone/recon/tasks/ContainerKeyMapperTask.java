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

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.apache.hadoop.utils.db.Table;
import org.apache.hadoop.utils.db.TableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to iterate over the OM DB and populate the Recon container DB with
 * the container -> Key reverse mapping.
 */
public class ContainerKeyMapperTask extends ReconDBUpdateTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerKeyMapperTask.class);

  private ContainerDBServiceProvider containerDBServiceProvider;
  private Collection<String> tables = new ArrayList<>();

  public ContainerKeyMapperTask(ContainerDBServiceProvider
                                    containerDBServiceProvider,
                                OMMetadataManager omMetadataManager) {
    super("ContainerKeyMapperTask");
    this.containerDBServiceProvider = containerDBServiceProvider;
    try {
      tables.add(omMetadataManager.getKeyTable().getName());
    } catch (IOException ioEx) {
      LOG.error("Unable to listen on Key Table updates ", ioEx);
    }
  }

  /**
   * Read Key -> ContainerId data from OM snapshot DB and write reverse map
   * (container, key) -> count to Recon Container DB.
   */
  @Override
  public Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager) {
    long omKeyCount = 0;
    try {
      LOG.info("Starting a 'reprocess' run of ContainerKeyMapperTask.");
      Instant start = Instant.now();

      // initialize new container DB
      containerDBServiceProvider.initNewContainerDB(new HashMap<>());

      Table<String, OmKeyInfo> omKeyInfoTable = omMetadataManager.getKeyTable();
      try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
               keyIter = omKeyInfoTable.iterator()) {
        while (keyIter.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
          OmKeyInfo omKeyInfo = kv.getValue();
          writeOMKeyToContainerDB(kv.getKey(), omKeyInfo);
          omKeyCount++;
        }
      }
      LOG.info("Completed 'reprocess' of ContainerKeyMapperTask.");
      Instant end = Instant.now();
      long duration = Duration.between(start, end).toMillis();
      LOG.info("It took me " + (double) duration / 1000.0 + " seconds to " +
          "process " + omKeyCount + " keys.");
    } catch (IOException ioEx) {
      LOG.error("Unable to populate Container Key Prefix data in Recon DB. ",
          ioEx);
      return new ImmutablePair<>(getTaskName(), false);
    }
    return new ImmutablePair<>(getTaskName(), true);
  }

  @Override
  protected Collection<String> getTaskTables() {
    return tables;
  }

  @Override
  Pair<String, Boolean> process(OMUpdateEventBatch events) {
    Iterator<OMDBUpdateEvent> eventIterator = events.getIterator();
    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, OmKeyInfo> omdbUpdateEvent = eventIterator.next();
      String updatedKey = omdbUpdateEvent.getKey();
      OmKeyInfo updatedKeyValue = omdbUpdateEvent.getValue();
      try {
        switch (omdbUpdateEvent.getAction()) {
        case PUT:
          writeOMKeyToContainerDB(updatedKey, updatedKeyValue);
          break;

        case DELETE:
          deleteOMKeyFromContainerDB(updatedKey);
          break;

        default: LOG.debug("Skipping DB update event : " + omdbUpdateEvent
            .getAction());
        }
      } catch (IOException e) {
        LOG.error("Unexpected exception while updating key data : {} ",
            updatedKey, e);
        return new ImmutablePair<>(getTaskName(), false);
      }
    }
    return new ImmutablePair<>(getTaskName(), true);
  }

  /**
   * Delete an OM Key from Container DB and update containerID -> no. of keys
   * count.
   *
   * @param key key String.
   * @throws IOException If Unable to write to container DB.
   */
  private void  deleteOMKeyFromContainerDB(String key)
      throws IOException {

    TableIterator<ContainerKeyPrefix, ? extends
        Table.KeyValue<ContainerKeyPrefix, Integer>> containerIterator =
        containerDBServiceProvider.getContainerTableIterator();

    Set<ContainerKeyPrefix> keysToBeDeleted = new HashSet<>();

    while (containerIterator.hasNext()) {
      Table.KeyValue<ContainerKeyPrefix, Integer> keyValue =
          containerIterator.next();
      String keyPrefix = keyValue.getKey().getKeyPrefix();
      if (keyPrefix.equals(key)) {
        keysToBeDeleted.add(keyValue.getKey());
      }
    }

    for (ContainerKeyPrefix containerKeyPrefix : keysToBeDeleted) {
      containerDBServiceProvider.deleteContainerMapping(containerKeyPrefix);

      // decrement count and update containerKeyCount.
      Long containerID = containerKeyPrefix.getContainerId();
      long keyCount =
          containerDBServiceProvider.getKeyCountForContainer(containerID);
      if (keyCount > 0) {
        containerDBServiceProvider.storeContainerKeyCount(containerID,
            --keyCount);
      }
    }
  }

  /**
   * Write an OM key to container DB and update containerID -> no. of keys
   * count.
   *
   * @param key key String
   * @param omKeyInfo omKeyInfo value
   * @throws IOException if unable to write to recon DB.
   */
  private void  writeOMKeyToContainerDB(String key, OmKeyInfo omKeyInfo)
      throws IOException {
    long containerCountToIncrement = 0;
    for (OmKeyLocationInfoGroup omKeyLocationInfoGroup : omKeyInfo
        .getKeyLocationVersions()) {
      long keyVersion = omKeyLocationInfoGroup.getVersion();
      for (OmKeyLocationInfo omKeyLocationInfo : omKeyLocationInfoGroup
          .getLocationList()) {
        long containerId = omKeyLocationInfo.getContainerID();
        ContainerKeyPrefix containerKeyPrefix = new ContainerKeyPrefix(
            containerId, key, keyVersion);
        if (containerDBServiceProvider.getCountForContainerKeyPrefix(
            containerKeyPrefix) == 0) {
          // Save on writes. No need to save same container-key prefix
          // mapping again.
          containerDBServiceProvider.storeContainerKeyMapping(
              containerKeyPrefix, 1);

          // check if container already exists and
          // increment the count of containers if it does not exist
          if (!containerDBServiceProvider.doesContainerExists(containerId)) {
            containerCountToIncrement++;
          }

          // update the count of keys for the given containerID
          long keyCount =
              containerDBServiceProvider.getKeyCountForContainer(containerId);

          // increment the count and update containerKeyCount.
          // keyCount will be 0 if containerID is not found. So, there is no
          // need to initialize keyCount for the first time.
          containerDBServiceProvider.storeContainerKeyCount(containerId,
              ++keyCount);
        }
      }
    }

    if (containerCountToIncrement > 0) {
      containerDBServiceProvider
          .incrementContainerCountBy(containerCountToIncrement);
    }
  }

}
