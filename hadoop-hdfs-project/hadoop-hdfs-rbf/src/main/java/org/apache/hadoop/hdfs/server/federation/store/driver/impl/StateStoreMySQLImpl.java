/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.store.driver.impl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.metrics.StateStoreMetrics;
import org.apache.hadoop.hdfs.server.federation.router.security.token.SQLConnectionFactory;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreOperationResult;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.DisabledNameservice;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.Query;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.server.federation.store.StateStoreUtils.*;

/**
 * StateStoreDriver implementation based on MySQL.
 * There is a separate table for each record type. Each table just as two
 * columns, recordKey and recordValue.
 */
public class StateStoreMySQLImpl extends StateStoreSerializableImpl {
  public static final String SQL_STATE_STORE_CONF_PREFIX = "state-store-mysql.";
  public static final String CONNECTION_URL =
      SQL_STATE_STORE_CONF_PREFIX + "connection.url";
  public static final String CONNECTION_USERNAME =
      SQL_STATE_STORE_CONF_PREFIX + "connection.username";
  public static final String CONNECTION_PASSWORD =
      SQL_STATE_STORE_CONF_PREFIX + "connection.password";
  public static final String CONNECTION_DRIVER =
      SQL_STATE_STORE_CONF_PREFIX + "connection.driver";

  private static final Logger LOG =
      LoggerFactory.getLogger(StateStoreMySQLImpl.class);
  private SQLConnectionFactory connectionFactory;
  /** If the driver has been initialized. */
  private boolean initialized = false;
  private final static Set<String> VALID_TABLES = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          MembershipState.class.getSimpleName(),
          RouterState.class.getSimpleName(),
          MountTable.class.getSimpleName(),
          DisabledNameservice.class.getSimpleName()
      ))
  );

  @Override
  public boolean initDriver() {
    Configuration conf = getConf();
    connectionFactory = new MySQLStateStoreHikariDataSourceConnectionFactory(conf);
    initialized = true;
    LOG.info("MySQL state store connection factory initialized");
    return true;
  }

  @Override
  public <T extends BaseRecord> boolean initRecordStorage(String className, Class<T> clazz) {
    String tableName = getAndValidateTableNameForClass(clazz);
    try (Connection connection = connectionFactory.getConnection();
        ResultSet resultSet = connection
            .getMetaData()
            .getTables(null, null, tableName, null)) {
      if (resultSet.next()) {
        return true;
      }
    } catch (SQLException e) {
      LOG.error("Could not check if table {} able exists", tableName);
    }

    try (Connection connection = connectionFactory.getConnection();
        Statement statement = connection.createStatement()) {
      String sql = String.format("CREATE TABLE %s ("
          + "recordKey VARCHAR (255) NOT NULL,"
          + "recordValue VARCHAR (2047) NOT NULL, "
          + "PRIMARY KEY(recordKey))", tableName);
      statement.execute(sql);
      return true;
    } catch (SQLException e) {
      LOG.error(String.format("Cannot create table %s for record type %s.",
          tableName, className), e.getMessage());
      return false;
    }
  }

  @Override
  public boolean isDriverReady() {
    return this.initialized;
  }

  @Override
  public void close() throws Exception {
    connectionFactory.shutdown();
  }

  @Override
  public <T extends BaseRecord> QueryResult<T> get(Class<T> clazz)
      throws IOException {
    String tableName = getAndValidateTableNameForClass(clazz);
    verifyDriverReady();
    long start = Time.monotonicNow();
    StateStoreMetrics metrics = getMetrics();
    List<T> ret = new ArrayList<>();
    try (Connection connection = connectionFactory.getConnection();
        PreparedStatement statement = connection.prepareStatement(
            String.format("SELECT * FROM %s", tableName))) {
      try (ResultSet result = statement.executeQuery()) {
        while(result.next()) {
          String recordValue = result.getString("recordValue");
          T record = newRecord(recordValue, clazz, false);
          ret.add(record);
        }
      }
    } catch (SQLException e) {
      if (metrics != null) {
        metrics.addFailure(Time.monotonicNow() - start);
      }
      String msg = "Cannot fetch records for " + clazz.getSimpleName();
      LOG.error(msg, e);
      throw new IOException(msg, e);
    }

    if (metrics != null) {
      metrics.addRead(Time.monotonicNow() - start);
    }
    return new QueryResult<>(ret, getTime());
  }

  @Override
  public <T extends BaseRecord> StateStoreOperationResult putAll(
      List<T> records, boolean allowUpdate, boolean errorIfExists) throws IOException {
    if (records.isEmpty()) {
      return StateStoreOperationResult.getDefaultSuccessResult();
    }

    verifyDriverReady();
    StateStoreMetrics metrics = getMetrics();

    long start = Time.monotonicNow();

    boolean success = true;
    final List<String> failedRecordsKeys = new ArrayList<>();
    for (T record : records) {
      String tableName = getAndValidateTableNameForClass(record.getClass());
      String primaryKey = getPrimaryKey(record);
      String data = serializeString(record);

      if (recordExists(tableName, primaryKey)) {
        if (allowUpdate) {
          // Update the mod time stamp. Many backends will use their
          // own timestamp for the mod time.
          record.setDateModified(this.getTime());
          if (!updateRecord(tableName, primaryKey, data)) {
            LOG.error("Cannot write {} into table {}", primaryKey, tableName);
            failedRecordsKeys.add(getOriginalPrimaryKey(primaryKey));
            success = false;
          }
        } else {
          if (errorIfExists) {
            LOG.error("Attempted to insert record {} that already exists "
                + "in table {} and updates are disallowed.", primaryKey, tableName);
            failedRecordsKeys.add(getOriginalPrimaryKey(primaryKey));
            success = false;
          } else {
            LOG.debug("Not updating {} as updates are not allowed", record);
          }
        }
      } else {
        if (!insertRecord(tableName, primaryKey, data)) {
          LOG.error("Cannot write {} in table {}", primaryKey, tableName);
          failedRecordsKeys.add(getOriginalPrimaryKey(primaryKey));
          success = false;
        }
      }
    }

    long end = Time.monotonicNow();
    if (metrics != null) {
      if (success) {
        metrics.addWrite(end - start);
      } else {
        metrics.addFailure(end - start);
      }
    }
    return new StateStoreOperationResult(failedRecordsKeys, success);
  }

  @Override
  public <T extends BaseRecord> boolean removeAll(Class<T> clazz) throws IOException {
    verifyDriverReady();
    long startTimeMs = Time.monotonicNow();
    StateStoreMetrics metrics = getMetrics();
    boolean success = true;
    String tableName = getAndValidateTableNameForClass(clazz);
    try (Connection connection = connectionFactory.getConnection(true);
        PreparedStatement truncateTable = connection.prepareStatement(
            String.format("TRUNCATE TABLE %s", tableName))){
      truncateTable.execute();
    } catch (SQLException e) {
      LOG.error("Could not remove all records in table {}", tableName, e);
      success = false;
    }

    if (metrics != null) {
      long durationMs = Time.monotonicNow() - startTimeMs;
      if (success) {
        metrics.addRemove(durationMs);
      } else {
        metrics.addFailure(durationMs);
      }
    }
    return success;
  }

  @Override
  public <T extends BaseRecord> int remove(Class<T> clazz, Query<T> query) throws IOException {
    verifyDriverReady();

    if (query == null) {
      return 0;
    }

    long startTimeMs = Time.monotonicNow();
    StateStoreMetrics metrics = getMetrics();
    int removed = 0;
    // Get the current records
    try {
      final QueryResult<T> result = get(clazz);
      final List<T> existingRecords = result.getRecords();
      // Write all of the existing records except those to be removed
      final List<T> recordsToRemove = filterMultiple(query, existingRecords);
      boolean success = true;
      for (T recordToRemove : recordsToRemove) {
        String tableName = getAndValidateTableNameForClass(clazz);
        String primaryKey = getPrimaryKey(recordToRemove);
        if (removeRecord(tableName, primaryKey)) {
          removed++;
        } else {
          LOG.error("Cannot remove record {} from table {}", primaryKey, tableName);
          success = false;
        }
      }
      if (!success) {
        LOG.error("Cannot remove records {} query {}", clazz, query);
        if (metrics != null) {
          metrics.addFailure(Time.monotonicNow() - startTimeMs);
        }
      }
    } catch (IOException e) {
      LOG.error("Cannot remove records {} query {}", clazz, query, e);
      if (metrics != null) {
        metrics.addFailure(Time.monotonicNow() - startTimeMs);
      }
    }

    if (removed > 0 && metrics != null) {
      metrics.addRemove(Time.monotonicNow() - startTimeMs);
    }
    return removed;
  }

  /**
   * Insert a record with a given key into the specified table.
   *
   * @param tableName Name of table to modify
   * @param key Primary key for the record.
   * @param data The record value for the given record key.
   * @return True is operation is successful, false otherwise.
   */
  protected boolean insertRecord(String tableName, String key, String data) {
    try (Connection connection = connectionFactory.getConnection(true);
        PreparedStatement statement = connection.prepareStatement(
            String.format("INSERT INTO %s (recordKey, recordValue) VALUES (?, ?)", tableName))) {
      statement.setString(1, key);
      statement.setString(2, data);
      statement.execute();
    } catch (SQLException e) {
      LOG.error("Failed to insert record {} into table {}", key, tableName, e);
      return false;
    }
    return true;
  }

  /**
   * Updates the record with a given key from the specified table.
   *
   * @param tableName Name of table to modify
   * @param key Primary key for the record.
   * @param data The record value for the given record key.
   * @return True is operation is successful, false otherwise.
   */
  protected boolean updateRecord(String tableName, String key, String data) {
    try (Connection connection = connectionFactory.getConnection(true);
        PreparedStatement statement = connection.prepareStatement(
            String.format("UPDATE %s SET recordValue = ? WHERE recordKey = ?", tableName))) {
      statement.setString(1, data);
      statement.setString(2, key);
      statement.execute();
    } catch (SQLException e){
      LOG.error("Failed to update record {} in table {}", key, tableName, e);
      return false;
    }
    return true;
  }

  /**
   * Checks if a record with a given key existing in the specified table.
   * @param tableName Name of table to modify
   * @param key Primary key for the record.
   * @return True is operation is successful, false otherwise.
   */
  protected boolean recordExists(String tableName, String key) {
    try (Connection connection = connectionFactory.getConnection(true);
        PreparedStatement statement = connection.prepareStatement(
            String.format("SELECT * FROM %s WHERE recordKey = ?", tableName))) {
      statement.setString(1, key);
      try (ResultSet result = statement.executeQuery()) {
        return result.next();
      }
    } catch (SQLException e) {
      LOG.error("Failed to check existence of record {} in table {}", key, tableName, e);
      return false;
    }
  }

  /**
   * Removes the record with a given key from the specified table.
   * @param tableName Name of table to modify
   * @param key Primary key for the record.
   * @return True is operation is successful, false otherwise.
   */
  protected boolean removeRecord(String tableName, String key) {
    try (Connection connection = connectionFactory.getConnection(true);
        PreparedStatement statement = connection.prepareStatement(
            String.format("DELETE FROM %s WHERE recordKey = ?", tableName))) {
      statement.setString(1, key);
      statement.execute();
      return true;
    } catch (SQLException e) {
      LOG.error("Failed to remove record {} in table {}", key, tableName, e);
      return false;
    }
  }

  /**
   * Get the table for a record class and validate is this is one of the supported
   * record types.
   * @param clazz Class of the record.
   * @return Table name for this record class.
   */
  private <T extends BaseRecord> String getAndValidateTableNameForClass(final Class<T> clazz) {
    String tableName = StateStoreUtils.getRecordName(clazz);
    if (VALID_TABLES.contains(tableName)) {
      return tableName;
    } else {
      throw new IllegalArgumentException(tableName + " is not a valid table name");
    }
  }


  /**
   * Class that relies on a HikariDataSource to provide SQL connections.
   */
  static class MySQLStateStoreHikariDataSourceConnectionFactory
      implements SQLConnectionFactory {
    protected final static String HIKARI_PROPS = SQL_STATE_STORE_CONF_PREFIX
        + "connection.hikari.";
    private final HikariDataSource dataSource;

    MySQLStateStoreHikariDataSourceConnectionFactory(Configuration conf) {
      Properties properties = new Properties();
      properties.setProperty("jdbcUrl", conf.get(StateStoreMySQLImpl.CONNECTION_URL));
      properties.setProperty("username", conf.get(StateStoreMySQLImpl.CONNECTION_USERNAME));
      properties.setProperty("password", conf.get(StateStoreMySQLImpl.CONNECTION_PASSWORD));
      properties.setProperty("driverClassName", conf.get(StateStoreMySQLImpl.CONNECTION_DRIVER));

      // Include hikari connection properties
      properties.putAll(conf.getPropsWithPrefix(HIKARI_PROPS));

      HikariConfig hikariConfig = new HikariConfig(properties);
      this.dataSource = new HikariDataSource(hikariConfig);
    }

    @Override
    public Connection getConnection() throws SQLException {
      return dataSource.getConnection();
    }

    @Override
    public void shutdown() {
      // Close database connections
      dataSource.close();
    }
  }

}