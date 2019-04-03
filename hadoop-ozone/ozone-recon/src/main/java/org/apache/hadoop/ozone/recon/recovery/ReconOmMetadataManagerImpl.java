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

package org.apache.hadoop.ozone.recon.recovery;

import java.io.File;
import java.io.IOException;

import javax.inject.Inject;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.utils.db.DBStore;
import org.apache.hadoop.utils.db.DBStoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon's implementation of the OM Metadata manager. By extending and
 * relying on the OmMetadataManagerImpl, we can make sure all changes made to
 * schema in OM will be automatically picked up by Recon.
 */
public class ReconOmMetadataManagerImpl extends OmMetadataManagerImpl
    implements ReconOMMetadataManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconOmMetadataManagerImpl.class);

  @Inject
  private OzoneConfiguration ozoneConfiguration;

  @Inject
  public ReconOmMetadataManagerImpl(OzoneConfiguration configuration) {
    this.ozoneConfiguration = configuration;
  }

  @Override
  public void start(OzoneConfiguration configuration) throws IOException {
    LOG.info("Starting ReconOMMetadataManagerImpl");
  }

  /**
   * Replace existing DB instance with new one.
   *
   * @param dbFile new DB file location.
   */
  private void initializeNewRdbStore(File dbFile) throws IOException {
    try {
      DBStoreBuilder dbStoreBuilder =
          DBStoreBuilder.newBuilder(ozoneConfiguration)
          .setReadOnly(true)
          .setName(dbFile.getName())
          .setPath(dbFile.toPath().getParent());
      addOMTablesAndCodecs(dbStoreBuilder);
      DBStore newStore = dbStoreBuilder.build();
      setStore(newStore);
      LOG.info("Created new OM DB snapshot at {}.",
          dbFile.getAbsolutePath());
    } catch (IOException ioEx) {
      LOG.error("Unable to initialize Recon OM DB snapshot store.",
          ioEx);
    }
    if (getStore() != null) {
      initializeOmTables();
    }
  }

  @Override
  public void updateOmDB(File newDbLocation) throws IOException {
    if (getStore() != null) {
      File oldDBLocation = getStore().getDbLocation();
      if (oldDBLocation.exists()) {
        LOG.info("Cleaning up old OM snapshot db at {}.",
            oldDBLocation.getAbsolutePath());
        FileUtils.deleteDirectory(oldDBLocation);
      }
    }
    initializeNewRdbStore(newDbLocation);
  }

}