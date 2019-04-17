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
package org.apache.hadoop.ozone.recon;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SQL_AUTO_COMMIT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SQL_CONNECTION_TIMEOUT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SQL_DB_DRIVER;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SQL_DB_JDBC_URL;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SQL_DB_PASSWORD;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SQL_DB_USER;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SQL_IDLE_CONNECTION_TEST_PERIOD;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SQL_MAX_ACTIVE_CONNECTIONS;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SQL_MAX_CONNECTION_AGE;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SQL_MAX_IDLE_CONNECTION_AGE;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SQL_MAX_IDLE_CONNECTION_TEST_STMT;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.persistence.DataSourceConfiguration;
import org.apache.hadoop.ozone.recon.persistence.JooqPersistenceModule;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOmMetadataManagerImpl;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.ReconContainerDBProvider;
import org.apache.hadoop.ozone.recon.spi.impl.ContainerDBServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.utils.db.DBStore;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

/**
 * Guice controller that defines concrete bindings.
 */
public class ReconControllerModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(OzoneConfiguration.class).toProvider(OzoneConfigurationProvider.class);
    bind(ReconHttpServer.class).in(Singleton.class);
    bind(DBStore.class)
        .toProvider(ReconContainerDBProvider.class).in(Singleton.class);
    bind(ReconOMMetadataManager.class)
        .to(ReconOmMetadataManagerImpl.class).in(Singleton.class);
    bind(ContainerDBServiceProvider.class)
        .to(ContainerDBServiceProviderImpl.class).in(Singleton.class);
    bind(OzoneManagerServiceProvider.class)
        .to(OzoneManagerServiceProviderImpl.class).in(Singleton.class);

    // Persistence - inject configuration provider
    install(new JooqPersistenceModule(
        getProvider(DataSourceConfiguration.class)));
  }

  @Provides
  DataSourceConfiguration getDataSourceConfiguration(
      final OzoneConfiguration ozoneConfiguration) {

    return new DataSourceConfiguration() {
      @Override
      public String getDriverClass() {
        return ozoneConfiguration.get(OZONE_RECON_SQL_DB_DRIVER,
            "org.sqlite.JDBC");
      }

      @Override
      public String getJdbcUrl() {
        return ozoneConfiguration.get(OZONE_RECON_SQL_DB_JDBC_URL);
      }

      @Override
      public String getUserName() {
        return ozoneConfiguration.get(OZONE_RECON_SQL_DB_USER);
      }

      @Override
      public String getPassword() {
        return ozoneConfiguration.get(OZONE_RECON_SQL_DB_PASSWORD);
      }

      @Override
      public boolean setAutoCommit() {
        return ozoneConfiguration.getBoolean(
            OZONE_RECON_SQL_AUTO_COMMIT, false);
      }

      @Override
      public long getConnectionTimeout() {
        return ozoneConfiguration.getLong(
            OZONE_RECON_SQL_CONNECTION_TIMEOUT, 30000);
      }

      @Override
      public String getSqlDialect() {
        return JooqPersistenceModule.DEFAULT_DIALECT.toString();
      }

      @Override
      public Integer getMaxActiveConnections() {
        return ozoneConfiguration.getInt(
            OZONE_RECON_SQL_MAX_ACTIVE_CONNECTIONS, 10);
      }

      @Override
      public Integer getMaxConnectionAge() {
        return ozoneConfiguration.getInt(
            OZONE_RECON_SQL_MAX_CONNECTION_AGE, 1800);
      }

      @Override
      public Integer getMaxIdleConnectionAge() {
        return ozoneConfiguration.getInt(
            OZONE_RECON_SQL_MAX_IDLE_CONNECTION_AGE, 3600);
      }

      @Override
      public String getConnectionTestStatement() {
        return ozoneConfiguration.get(
            OZONE_RECON_SQL_MAX_IDLE_CONNECTION_TEST_STMT, "SELECT 1");
      }

      @Override
      public Integer getIdleConnectionTestPeriod() {
        return ozoneConfiguration.getInt(
            OZONE_RECON_SQL_IDLE_CONNECTION_TEST_PERIOD, 60);
      }
    };

  }
}
