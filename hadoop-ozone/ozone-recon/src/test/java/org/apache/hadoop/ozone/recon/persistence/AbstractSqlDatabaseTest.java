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
package org.apache.hadoop.ozone.recon.persistence;

import java.io.File;
import java.io.IOException;

import javax.sql.DataSource;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provider;

/**
 * Create an injector for tests that need to access the SQl database.
 */
public abstract class AbstractSqlDatabaseTest {

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();
  private static File tempDir;

  private static Injector injector;
  private static DSLContext dslContext;

  @BeforeClass
  public static void setup() throws IOException {
    tempDir = temporaryFolder.newFolder();

    DataSourceConfigurationProvider configurationProvider =
        new DataSourceConfigurationProvider();
    JooqPersistenceModule persistenceModule =
        new JooqPersistenceModule(configurationProvider);

    injector = Guice.createInjector(persistenceModule, new AbstractModule() {
      @Override
      public void configure() {
        bind(DataSourceConfiguration.class).toProvider(configurationProvider);
        }
    });
    dslContext = DSL.using(new DefaultConfiguration().set(
        injector.getInstance(DataSource.class)));
  }

  @AfterClass
  public static void tearDown() {
    temporaryFolder.delete();
  }

  protected Injector getInjector() {
    return injector;
  }

  protected DSLContext getDslContext() {
    return dslContext;
  }

  static class DataSourceConfigurationProvider implements
      Provider<DataSourceConfiguration> {

    @Override
    public DataSourceConfiguration get() {
      return new DataSourceConfiguration() {
        @Override
        public String getDriverClass() {
          return "org.sqlite.JDBC";
        }

        @Override
        public String getJdbcUrl() {
          return "jdbc:sqlite:" + tempDir.getAbsolutePath() +
              File.separator + "sqlite_recon.db";
        }

        @Override
        public String getUserName() {
          return null;
        }

        @Override
        public String getPassword() {
          return null;
        }

        @Override
        public boolean setAutoCommit() {
          return true;
        }

        @Override
        public long getConnectionTimeout() {
          return 10000;
        }

        @Override
        public String getSqlDialect() {
          return SQLDialect.SQLITE.toString();
        }

        @Override
        public Integer getMaxActiveConnections() {
          return 2;
        }

        @Override
        public Integer getMaxConnectionAge() {
          return 120;
        }

        @Override
        public Integer getMaxIdleConnectionAge() {
          return 120;
        }

        @Override
        public String getConnectionTestStatement() {
          return "SELECT 1";
        }

        @Override
        public Integer getIdleConnectionTestPeriod() {
          return 30;
        }
      };
    }
  }
}
