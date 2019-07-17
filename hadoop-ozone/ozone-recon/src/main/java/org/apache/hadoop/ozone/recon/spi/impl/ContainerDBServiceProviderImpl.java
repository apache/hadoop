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

package org.apache.hadoop.ozone.recon.spi.impl;

import static org.apache.hadoop.ozone.recon.ReconConstants.CONTAINER_COUNT_KEY;
import static org.apache.hadoop.ozone.recon.ReconConstants.CONTAINER_KEY_COUNT_TABLE;
import static org.apache.hadoop.ozone.recon.ReconConstants.CONTAINER_KEY_TABLE;
import static org.jooq.impl.DSL.currentTimestamp;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.using;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.api.types.ContainerMetadata;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.apache.hadoop.utils.db.DBStore;
import org.apache.hadoop.utils.db.Table;
import org.apache.hadoop.utils.db.Table.KeyValue;
import org.apache.hadoop.utils.db.TableIterator;
import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
import org.hadoop.ozone.recon.schema.tables.pojos.GlobalStats;
import org.jooq.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the Recon Container DB Service.
 */
@Singleton
public class ContainerDBServiceProviderImpl
    implements ContainerDBServiceProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerDBServiceProviderImpl.class);

  private Table<ContainerKeyPrefix, Integer> containerKeyTable;
  private Table<Long, Long> containerKeyCountTable;
  private GlobalStatsDao globalStatsDao;

  @Inject
  private OzoneConfiguration configuration;

  @Inject
  private DBStore containerDbStore;

  @Inject
  private Configuration sqlConfiguration;

  @Inject
  public ContainerDBServiceProviderImpl(DBStore dbStore,
                                        Configuration sqlConfiguration) {
    globalStatsDao = new GlobalStatsDao(sqlConfiguration);
    try {
      this.containerKeyTable = dbStore.getTable(CONTAINER_KEY_TABLE,
          ContainerKeyPrefix.class, Integer.class);
      this.containerKeyCountTable = dbStore.getTable(CONTAINER_KEY_COUNT_TABLE,
          Long.class, Long.class);
    } catch (IOException e) {
      LOG.error("Unable to create Container Key tables." + e);
    }
  }

  /**
   * Initialize a new container DB instance, getting rid of the old instance
   * and then storing the passed in container prefix counts into the created
   * DB instance. Also, truncate or reset the SQL tables as required.
   * @param containerKeyPrefixCounts Map of container key-prefix to
   *                                 number of keys with the prefix.
   * @throws IOException
   */
  @Override
  public void initNewContainerDB(Map<ContainerKeyPrefix, Integer>
                                     containerKeyPrefixCounts)
      throws IOException {

    File oldDBLocation = containerDbStore.getDbLocation();
    containerDbStore = ReconContainerDBProvider.getNewDBStore(configuration);
    containerKeyTable = containerDbStore.getTable(CONTAINER_KEY_TABLE,
        ContainerKeyPrefix.class, Integer.class);

    if (oldDBLocation.exists()) {
      LOG.info("Cleaning up old Recon Container DB at {}.",
          oldDBLocation.getAbsolutePath());
      FileUtils.deleteDirectory(oldDBLocation);
    }

    if (containerKeyPrefixCounts != null) {
      for (Map.Entry<ContainerKeyPrefix, Integer> entry :
          containerKeyPrefixCounts.entrySet()) {
        containerKeyTable.put(entry.getKey(), entry.getValue());
      }
    }

    // reset total count of containers to zero
    storeContainerCount(0L);
  }

  /**
   * Concatenate the containerID and Key Prefix using a delimiter and store the
   * count into the container DB store.
   *
   * @param containerKeyPrefix the containerID, key-prefix tuple.
   * @param count Count of the keys matching that prefix.
   * @throws IOException
   */
  @Override
  public void storeContainerKeyMapping(ContainerKeyPrefix containerKeyPrefix,
                                       Integer count)
      throws IOException {
    containerKeyTable.put(containerKeyPrefix, count);
  }

  /**
   * Store the containerID -> no. of keys count into the container DB store.
   *
   * @param containerID the containerID.
   * @param count count of the keys within the given containerID.
   * @throws IOException
   */
  @Override
  public void storeContainerKeyCount(Long containerID, Long count)
      throws IOException {
    containerKeyCountTable.put(containerID, count);
  }

  /**
   * Get the total count of keys within the given containerID.
   *
   * @param containerID the given containerID.
   * @return count of keys within the given containerID.
   * @throws IOException
   */
  @Override
  public long getKeyCountForContainer(Long containerID) throws IOException {
    Long keyCount = containerKeyCountTable.get(containerID);
    return keyCount == null ? 0L : keyCount;
  }

  /**
   * Get if a containerID exists or not.
   *
   * @param containerID the given containerID.
   * @return if the given ContainerID exists or not.
   * @throws IOException
   */
  @Override
  public boolean doesContainerExists(Long containerID) throws IOException {
    return containerKeyCountTable.get(containerID) != null;
  }

  /**
   * Put together the key from the passed in object and get the count from
   * the container DB store.
   *
   * @param containerKeyPrefix the containerID, key-prefix tuple.
   * @return count of keys matching the containerID, key-prefix.
   * @throws IOException
   */
  @Override
  public Integer getCountForContainerKeyPrefix(
      ContainerKeyPrefix containerKeyPrefix) throws IOException {
    Integer count =  containerKeyTable.get(containerKeyPrefix);
    return count == null ? Integer.valueOf(0) : count;
  }

  /**
   * Get key prefixes for the given container ID.
   *
   * @param containerId the given containerID.
   * @return Map of (Key-Prefix,Count of Keys).
   */
  @Override
  public Map<ContainerKeyPrefix, Integer> getKeyPrefixesForContainer(
      long containerId) throws IOException {
    // set the default startKeyPrefix to empty string
    return getKeyPrefixesForContainer(containerId, StringUtils.EMPTY);
  }

  /**
   * Use the DB's prefix seek iterator to start the scan from the given
   * container ID and prev key prefix. The prev key prefix is skipped from
   * the result.
   *
   * @param containerId the given containerId.
   * @param prevKeyPrefix the given key prefix to start the scan from.
   * @return Map of (Key-Prefix,Count of Keys).
   */
  @Override
  public Map<ContainerKeyPrefix, Integer> getKeyPrefixesForContainer(
      long containerId, String prevKeyPrefix) throws IOException {

    Map<ContainerKeyPrefix, Integer> prefixes = new LinkedHashMap<>();
    TableIterator<ContainerKeyPrefix, ? extends KeyValue<ContainerKeyPrefix,
        Integer>> containerIterator = containerKeyTable.iterator();
    ContainerKeyPrefix seekKey;
    boolean skipPrevKey = false;
    if (StringUtils.isNotBlank(prevKeyPrefix)) {
      skipPrevKey = true;
      seekKey = new ContainerKeyPrefix(containerId, prevKeyPrefix);
    } else {
      seekKey = new ContainerKeyPrefix(containerId);
    }
    KeyValue<ContainerKeyPrefix, Integer> seekKeyValue =
        containerIterator.seek(seekKey);

    // check if RocksDB was able to seek correctly to the given key prefix
    // if not, then return empty result
    // In case of an empty prevKeyPrefix, all the keys in the container are
    // returned
    if (seekKeyValue == null ||
        (StringUtils.isNotBlank(prevKeyPrefix) &&
            !seekKeyValue.getKey().getKeyPrefix().equals(prevKeyPrefix))) {
      return prefixes;
    }

    while (containerIterator.hasNext()) {
      KeyValue<ContainerKeyPrefix, Integer> keyValue = containerIterator.next();
      ContainerKeyPrefix containerKeyPrefix = keyValue.getKey();

      // skip the prev key if prev key is present
      if (skipPrevKey &&
          containerKeyPrefix.getKeyPrefix().equals(prevKeyPrefix)) {
        continue;
      }

      // The prefix seek only guarantees that the iterator's head will be
      // positioned at the first prefix match. We still have to check the key
      // prefix.
      if (containerKeyPrefix.getContainerId() == containerId) {
        if (StringUtils.isNotEmpty(containerKeyPrefix.getKeyPrefix())) {
          prefixes.put(new ContainerKeyPrefix(containerId,
              containerKeyPrefix.getKeyPrefix(),
              containerKeyPrefix.getKeyVersion()),
              keyValue.getValue());
        } else {
          LOG.warn("Null key prefix returned for containerId = " + containerId);
        }
      } else {
        break; //Break when the first mismatch occurs.
      }
    }
    return prefixes;
  }

  /**
   * Iterate the DB to construct a Map of containerID -> containerMetadata
   * only for the given limit from the given start key. The start containerID
   * is skipped from the result.
   *
   * Return all the containers if limit < 0.
   *
   * @param limit No of containers to get.
   * @param prevContainer containerID after which the
   *                      list of containers are scanned.
   * @return Map of containerID -> containerMetadata.
   * @throws IOException
   */
  @Override
  public Map<Long, ContainerMetadata> getContainers(int limit,
                                                    long prevContainer)
      throws IOException {
    Map<Long, ContainerMetadata> containers = new LinkedHashMap<>();
    TableIterator<ContainerKeyPrefix, ? extends KeyValue<ContainerKeyPrefix,
        Integer>> containerIterator = containerKeyTable.iterator();
    ContainerKeyPrefix seekKey;
    if (prevContainer > 0L) {
      seekKey = new ContainerKeyPrefix(prevContainer);
      KeyValue<ContainerKeyPrefix,
          Integer> seekKeyValue = containerIterator.seek(seekKey);
      // Check if RocksDB was able to correctly seek to the given
      // prevContainer containerId. If not, then return empty result
      if (seekKeyValue != null &&
          seekKeyValue.getKey().getContainerId() != prevContainer) {
        return containers;
      } else {
        // seek to the prevContainer+1 containerID to start scan
        seekKey = new ContainerKeyPrefix(prevContainer + 1);
        containerIterator.seek(seekKey);
      }
    }
    while (containerIterator.hasNext()) {
      KeyValue<ContainerKeyPrefix, Integer> keyValue = containerIterator.next();
      ContainerKeyPrefix containerKeyPrefix = keyValue.getKey();
      Long containerID = containerKeyPrefix.getContainerId();
      Integer numberOfKeys = keyValue.getValue();

      // break the loop if limit has been reached
      // and one more new entity needs to be added to the containers map
      if (containers.size() == limit && !containers.containsKey(containerID)) {
        break;
      }

      // initialize containerMetadata with 0 as number of keys.
      containers.computeIfAbsent(containerID, ContainerMetadata::new);
      // increment number of keys for the containerID
      ContainerMetadata containerMetadata = containers.get(containerID);
      containerMetadata.setNumberOfKeys(containerMetadata.getNumberOfKeys() +
          numberOfKeys);
      containers.put(containerID, containerMetadata);
    }
    return containers;
  }

  @Override
  public void deleteContainerMapping(ContainerKeyPrefix containerKeyPrefix)
      throws IOException {
    containerKeyTable.delete(containerKeyPrefix);
  }

  /**
   * Get total count of containers.
   *
   * @return total count of containers.
   */
  @Override
  public long getCountForContainers() {
    GlobalStats containerCountRecord =
        globalStatsDao.fetchOneByKey(CONTAINER_COUNT_KEY);

    return (containerCountRecord == null) ? 0L :
        containerCountRecord.getValue();
  }

  @Override
  public TableIterator getContainerTableIterator() {
    return containerKeyTable.iterator();
  }

  /**
   * Store the total count of containers into the container DB store.
   *
   * @param count count of the containers present in the system.
   */
  @Override
  public void storeContainerCount(Long count) {
    // Get the current timestamp
    Timestamp now =
        using(sqlConfiguration).fetchValue(select(currentTimestamp()));
    GlobalStats containerCountRecord =
        globalStatsDao.fetchOneByKey(CONTAINER_COUNT_KEY);
    GlobalStats globalStatsRecord =
        new GlobalStats(CONTAINER_COUNT_KEY, count, now);

    // Insert a new record for CONTAINER_COUNT_KEY if it does not exist
    if (containerCountRecord == null) {
      globalStatsDao.insert(globalStatsRecord);
    } else {
      globalStatsDao.update(globalStatsRecord);
    }
  }

  /**
   * Increment the total count for containers in the system by the given count.
   *
   * @param count no. of new containers to add to containers total count.
   */
  @Override
  public void incrementContainerCountBy(long count) {
    long containersCount = getCountForContainers();
    storeContainerCount(containersCount + count);
  }
}