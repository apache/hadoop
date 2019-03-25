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

import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_CONTAINER_DB;
import static org.apache.hadoop.ozone.recon.ReconConstants.CONTAINER_KEY_TABLE;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconUtils.getReconDbDir;

import java.nio.file.Path;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.utils.db.DBStore;
import org.apache.hadoop.utils.db.DBStoreBuilder;
import org.apache.hadoop.utils.db.IntegerCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.ProvisionException;

/**
 * Provider for the Recon container DB (Metadata store).
 */
public class ReconContainerDBProvider implements Provider<DBStore> {

  @VisibleForTesting
  private static final Logger LOG =
      LoggerFactory.getLogger(ReconContainerDBProvider.class);

  @Inject
  private OzoneConfiguration configuration;

  @Override
  public DBStore get() {
    DBStore dbStore = getNewDBStore(configuration);
    if (dbStore == null) {
      throw new ProvisionException("Unable to provide instance of DBStore " +
          "store.");
    }
    return dbStore;
  }

  public static DBStore getNewDBStore(OzoneConfiguration configuration) {
    DBStore dbStore = null;
    String dbName = RECON_CONTAINER_DB + "_" + System.currentTimeMillis();
    try {
      Path metaDir = getReconDbDir(configuration, OZONE_RECON_DB_DIR).toPath();
      dbStore = DBStoreBuilder.newBuilder(configuration)
          .setPath(metaDir)
          .setName(dbName)
          .addTable(CONTAINER_KEY_TABLE)
          .addCodec(ContainerKeyPrefix.class, new ContainerKeyPrefixCodec())
          .addCodec(Integer.class, new IntegerCodec())
          .build();
    } catch (Exception ex) {
      LOG.error("Unable to initialize Recon container metadata store.", ex);
    }
    return dbStore;
  }
}
