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

import static com.google.inject.matcher.Matchers.annotatedWith;
import static com.google.inject.matcher.Matchers.any;

import java.sql.Connection;
import javax.sql.DataSource;

import org.jooq.Configuration;
import org.jooq.ConnectionProvider;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultConfiguration;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.transaction.annotation.Transactional;

import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;

/**
 * Persistence module that provides binding for {@link DataSource} and
 * a MethodInterceptor for nested transactions support.
 */
public class JooqPersistenceModule extends AbstractModule {

  private Provider<DataSourceConfiguration> configurationProvider;
  public static final SQLDialect DEFAULT_DIALECT = SQLDialect.SQLITE;

  public JooqPersistenceModule(
      Provider<DataSourceConfiguration> configurationProvider) {
    this.configurationProvider = configurationProvider;
  }

  @Override
  protected void configure() {
    bind(DataSource.class).toProvider(DefaultDataSourceProvider.class)
        .in(Singleton.class);

    TransactionalMethodInterceptor interceptor =
        new TransactionalMethodInterceptor(
            getProvider(DataSourceTransactionManager.class));

    bindInterceptor(annotatedWith(Transactional.class), any(), interceptor);
    bindInterceptor(any(), annotatedWith(Transactional.class), interceptor);
  }

  @Provides
  @Singleton
  Configuration getConfiguration(DefaultDataSourceProvider provider) {
    DataSource dataSource = provider.get();

    return new DefaultConfiguration()
        .set(dataSource)
        .set(new SpringConnectionProvider(dataSource))
        .set(SQLDialect.valueOf(configurationProvider.get().getSqlDialect()));
  }

  @Provides
  @Singleton
  DataSourceTransactionManager provideDataSourceTransactionManager(
      DataSource dataSource) {
    return new DataSourceTransactionManager(
        new TransactionAwareDataSourceProxy(dataSource));
  }

  /**
   * This connection provider uses Spring to extract the
   * {@link TransactionAwareDataSourceProxy} from our BoneCP pooled connection
   * {@link DataSource}.
   */
  static class SpringConnectionProvider implements ConnectionProvider {

    private final DataSource dataSource;

    SpringConnectionProvider(DataSource dataSource) {
      this.dataSource = dataSource;
    }

    @Override
    public Connection acquire() throws DataAccessException {
      return DataSourceUtils.getConnection(dataSource);
    }

    @Override
    public void release(Connection connection) throws DataAccessException {
      DataSourceUtils.releaseConnection(connection, dataSource);
    }
  }
}
