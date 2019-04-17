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

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;
import org.apache.hadoop.utils.db.Table;
import org.apache.hadoop.utils.db.TableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to iterate over the OM DB and populate the Recon container DB with
 * the container -> Key reverse mapping.
 */
public class ContainerKeyMapperTask implements Runnable {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerKeyMapperTask.class);

  private OzoneManagerServiceProvider ozoneManagerServiceProvider;
  private ContainerDBServiceProvider containerDBServiceProvider;

  public ContainerKeyMapperTask(
      OzoneManagerServiceProvider ozoneManagerServiceProvider,
      ContainerDBServiceProvider containerDBServiceProvider) {
    this.ozoneManagerServiceProvider = ozoneManagerServiceProvider;
    this.containerDBServiceProvider = containerDBServiceProvider;
  }

  /**
   * Read Key -> ContainerId data from OM snapshot DB and write reverse map
   * (container, key) -> count to Recon Container DB.
   */
  @Override
  public void run() {
    int omKeyCount = 0;
    int containerCount = 0;
    try {
      LOG.info("Starting a run of ContainerKeyMapperTask.");
      Instant start = Instant.now();

      //Update OM DB Snapshot.
      ozoneManagerServiceProvider.updateReconOmDBWithNewSnapshot();

      OMMetadataManager omMetadataManager = ozoneManagerServiceProvider
          .getOMMetadataManagerInstance();
      Table<String, OmKeyInfo> omKeyInfoTable = omMetadataManager.getKeyTable();
      try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
               keyIter = omKeyInfoTable.iterator()) {
        while (keyIter.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
          StringBuilder key = new StringBuilder(kv.getKey());
          OmKeyInfo omKeyInfo = kv.getValue();
          for (OmKeyLocationInfoGroup omKeyLocationInfoGroup : omKeyInfo
              .getKeyLocationVersions()) {
            long keyVersion = omKeyLocationInfoGroup.getVersion();
            for (OmKeyLocationInfo omKeyLocationInfo : omKeyLocationInfoGroup
                .getLocationList()) {
              long containerId = omKeyLocationInfo.getContainerID();
              ContainerKeyPrefix containerKeyPrefix = new ContainerKeyPrefix(
                  containerId, key.toString(), keyVersion);
              if (containerDBServiceProvider.getCountForForContainerKeyPrefix(
                  containerKeyPrefix) == 0) {
                // Save on writes. No need to save same container-key prefix
                // mapping again.
                containerDBServiceProvider.storeContainerKeyMapping(
                    containerKeyPrefix, 1);
              }
              containerCount++;
            }
          }
          omKeyCount++;
        }
      }
      LOG.info("Completed the run of ContainerKeyMapperTask.");
      Instant end = Instant.now();
      long duration = Duration.between(start, end).toMillis();
      LOG.info("It took me " + (double)duration / 1000.0 + " seconds to " +
          "process " + omKeyCount + " keys and " + containerCount + " " +
          "containers.");
    } catch (IOException ioEx) {
      LOG.error("Unable to populate Container Key Prefix data in Recon DB. ",
          ioEx);
    }
  }
}
