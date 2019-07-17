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

package org.apache.hadoop.ozone.recon.types;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.persistence.AbstractSqlDatabaseTest;
import org.apache.hadoop.ozone.recon.persistence.DataSourceConfiguration;
import org.apache.hadoop.ozone.recon.persistence.JooqPersistenceModule;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.ContainerDBServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.ReconContainerDBProvider;
import org.apache.hadoop.utils.db.DBStore;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_DB_DIR;

/**
 * Utility methods to get guice injector and ozone configuration.
 */
public interface GuiceInjectorUtilsForTests {

  /**
   * Get Guice Injector with bindings.
   * @param ozoneManagerServiceProvider
   * @param reconOMMetadataManager
   * @param temporaryFolder
   * @return Injector
   * @throws IOException ioEx.
   */
  default Injector getInjector(
      OzoneManagerServiceProviderImpl ozoneManagerServiceProvider,
      ReconOMMetadataManager reconOMMetadataManager,
      TemporaryFolder temporaryFolder
  ) throws IOException {

    File tempDir = temporaryFolder.newFolder();
    AbstractSqlDatabaseTest.DataSourceConfigurationProvider
        configurationProvider =
        new AbstractSqlDatabaseTest.DataSourceConfigurationProvider(tempDir);

    JooqPersistenceModule jooqPersistenceModule =
        new JooqPersistenceModule(configurationProvider);

    return Guice.createInjector(jooqPersistenceModule,
        new AbstractModule() {
          @Override
          protected void configure() {
            try {
              bind(DataSourceConfiguration.class)
                  .toProvider(configurationProvider);
              bind(OzoneConfiguration.class).toInstance(
                  getTestOzoneConfiguration(temporaryFolder));

              if (reconOMMetadataManager != null) {
                bind(ReconOMMetadataManager.class)
                    .toInstance(reconOMMetadataManager);
              }

              if (ozoneManagerServiceProvider != null) {
                bind(OzoneManagerServiceProvider.class)
                    .toInstance(ozoneManagerServiceProvider);
              }

              bind(DBStore.class).toProvider(ReconContainerDBProvider.class).
                  in(Singleton.class);
              bind(ContainerDBServiceProvider.class).to(
                  ContainerDBServiceProviderImpl.class).in(Singleton.class);
            } catch (IOException e) {
              Assert.fail();
            }
          }
        });
  }

  /**
   * Get Test OzoneConfiguration instance.
   * @return OzoneConfiguration
   * @throws IOException ioEx.
   */
  default OzoneConfiguration getTestOzoneConfiguration(
      TemporaryFolder temporaryFolder) throws IOException {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OZONE_RECON_OM_SNAPSHOT_DB_DIR,
        temporaryFolder.newFolder().getAbsolutePath());
    configuration.set(OZONE_RECON_DB_DIR, temporaryFolder.newFolder()
        .getAbsolutePath());
    return configuration;
  }
}
