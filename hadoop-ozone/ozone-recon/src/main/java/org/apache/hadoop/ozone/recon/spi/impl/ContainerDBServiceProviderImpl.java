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

import static org.apache.hadoop.ozone.recon.ReconConstants.CONTAINER_KEY_TABLE;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
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

  @Inject
  private OzoneConfiguration configuration;

  @Inject
  private DBStore containerDbStore;

  @Inject
  public ContainerDBServiceProviderImpl(DBStore dbStore) {
    try {
      this.containerKeyTable = dbStore.getTable(CONTAINER_KEY_TABLE,
          ContainerKeyPrefix.class, Integer.class);
    } catch (IOException e) {
      LOG.error("Unable to create Container Key Table. " + e);
    }
  }

  /**
   * Initialize a new container DB instance, getting rid of the old instance
   * and then storing the passed in container prefix counts into the created
   * DB instance.
   * @param containerKeyPrefixCounts Map of containerId, key-prefix tuple to
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
    for (Map.Entry<ContainerKeyPrefix, Integer> entry :
        containerKeyPrefixCounts.entrySet()) {
      containerKeyTable.put(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Concatenate the containerId and Key Prefix using a delimiter and store the
   * count into the container DB store.
   *
   * @param containerKeyPrefix the containerId, key-prefix tuple.
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
   * Put together the key from the passed in object and get the count from
   * the container DB store.
   *
   * @param containerKeyPrefix the containerId, key-prefix tuple.
   * @return count of keys matching the containerId, key-prefix.
   * @throws IOException
   */
  @Override
  public Integer getCountForForContainerKeyPrefix(
      ContainerKeyPrefix containerKeyPrefix) throws IOException {
    Integer count =  containerKeyTable.get(containerKeyPrefix);
    return count == null ? Integer.valueOf(0) : count;
  }

  /**
   * Use the DB's prefix seek iterator to start the scan from the given
   * container ID prefix.
   *
   * @param containerId the given containerId.
   * @return Map of (Key-Prefix,Count of Keys).
   */
  @Override
  public Map<ContainerKeyPrefix, Integer> getKeyPrefixesForContainer(
      long containerId) throws IOException {

    Map<ContainerKeyPrefix, Integer> prefixes = new HashMap<>();
    TableIterator<ContainerKeyPrefix, ? extends KeyValue<ContainerKeyPrefix,
        Integer>> containerIterator = containerKeyTable.iterator();
    containerIterator.seek(new ContainerKeyPrefix(containerId));
    while (containerIterator.hasNext()) {
      KeyValue<ContainerKeyPrefix, Integer> keyValue = containerIterator.next();
      ContainerKeyPrefix containerKeyPrefix = keyValue.getKey();
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
   * Iterate the DB to construct a Map of containerID -> containerMetadata.
   *
   * @return Map of containerID -> containerMetadata.
   * @throws IOException
   */
  @Override
  public Map<Long, ContainerMetadata> getContainers() throws IOException {
    Map<Long, ContainerMetadata> containers = new LinkedHashMap<>();
    TableIterator<ContainerKeyPrefix, ? extends KeyValue<ContainerKeyPrefix,
        Integer>> containerIterator = containerKeyTable.iterator();
    while (containerIterator.hasNext()) {
      KeyValue<ContainerKeyPrefix, Integer> keyValue = containerIterator.next();
      Long containerID = keyValue.getKey().getContainerId();
      Integer numberOfKeys = keyValue.getValue();

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
}