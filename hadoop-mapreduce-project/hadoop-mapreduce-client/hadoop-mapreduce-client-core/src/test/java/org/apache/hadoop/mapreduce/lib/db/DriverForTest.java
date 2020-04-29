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
package org.apache.hadoop.mapreduce.lib.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * class emulates a connection to database
 * 
 */
public class DriverForTest implements Driver {

  public static Connection getConnection() {
    Connection connection = mock(FakeConnection.class);
    try {
      Statement statement = mock(Statement.class);
      ResultSet results = mock(ResultSet.class);
      when(results.getLong(1)).thenReturn(15L);
      when(statement.executeQuery(any(String.class))).thenReturn(results);
      when(connection.createStatement()).thenReturn(statement);

      DatabaseMetaData metadata = mock(DatabaseMetaData.class);
      when(metadata.getDatabaseProductName()).thenReturn("Test");
      when(connection.getMetaData()).thenReturn(metadata);

      PreparedStatement reparedStatement0= mock(PreparedStatement.class);
      when(connection.prepareStatement(anyString())).thenReturn(
          reparedStatement0);

      PreparedStatement preparedStatement = mock(PreparedStatement.class);
      ResultSet resultSet = mock(ResultSet.class);
      when(resultSet.next()).thenReturn(false);
      when(preparedStatement.executeQuery()).thenReturn(resultSet);

      when(connection.prepareStatement(anyString(), anyInt(), anyInt()))
          .thenReturn(preparedStatement);
    } catch (SQLException e) {
      ;
    }
    return connection;
  }

  @Override
  public boolean acceptsURL(String arg0) throws SQLException {
    return "testUrl".equals(arg0);
  }

  @Override
  public Connection connect(String arg0, Properties arg1) throws SQLException {
   
    return getConnection();
  }

  @Override
  public int getMajorVersion() {
    return 1;
  }

  @Override
  public int getMinorVersion() {
    return 1;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String arg0, Properties arg1)
      throws SQLException {

    return null;
  }

  @Override
  public boolean jdbcCompliant() {
    return true;
  }
  
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  private interface FakeConnection extends Connection{
    public void setSessionTimeZone(String arg);
  }
  
}
