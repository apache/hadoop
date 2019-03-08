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

package org.apache.hadoop.ozone.recon.spi;

import static org.apache.hadoop.ozone.recon.ReconConstants.
    RECON_CONTAINER_DB;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.
    OZONE_RECON_CONTAINER_DB_CACHE_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.
    OZONE_RECON_CONTAINER_DB_CACHE_SIZE_MB;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.
    OZONE_RECON_DB_DIRS;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.utils.MetadataStoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Provider;

/**
 * Provider for the Recon container DB (Metadata store).
 */
public class ReconContainerDBProvider implements
    Provider<MetadataStore> {

  @VisibleForTesting
  private static final Logger LOG =
      LoggerFactory.getLogger(ReconContainerDBProvider.class);

  @Inject
  private OzoneConfiguration configuration;

  @Override
  public MetadataStore get() {
    File metaDir = ServerUtils.getDirWithFallBackToOzoneMetadata(configuration,
        OZONE_RECON_DB_DIRS, "Recon");
    File containerDBPath = new File(metaDir, RECON_CONTAINER_DB);
    int cacheSize = configuration.getInt(OZONE_RECON_CONTAINER_DB_CACHE_SIZE_MB,
        OZONE_RECON_CONTAINER_DB_CACHE_SIZE_DEFAULT);

    try {
      return MetadataStoreBuilder.newBuilder()
          .setConf(configuration)
          .setDbFile(containerDBPath)
          .setCacheSize(cacheSize * OzoneConsts.MB)
          .build();
    } catch (IOException ioEx) {
      LOG.error("Unable to initialize Recon container metadata store.", ioEx);
    }
    return null;
  }
}
