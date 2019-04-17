/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.audit.parser.common;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.audit.parser.model.AuditEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Database helper for ozone audit parser tool.
 */
public final class DatabaseHelper {
  private DatabaseHelper() {
    //Never constructed
  }
  static {
    loadProperties();
  }
  private static final Logger LOG =
      LoggerFactory.getLogger(DatabaseHelper.class);
  private static Map<String, String> properties;

  public static boolean setup(String dbName, String logs) {
    //loadProperties();
    if(createAuditTable(dbName)) {
      return insertAudits(dbName, logs);
    } else {
      return false;
    }
  }

  private static Connection getConnection(String dbName) {

    Connection connection = null;
    try{
      Class.forName(ParserConsts.DRIVER);
      connection = DriverManager.getConnection(
          ParserConsts.CONNECTION_PREFIX + dbName);
    } catch (ClassNotFoundException e) {
      LOG.error(e.getMessage());
    } catch (SQLException e) {
      LOG.error(e.getMessage());
    }
    return connection;
  }

  private static void loadProperties() {
    Properties props = new Properties();
    try{
      InputStream inputStream = DatabaseHelper.class.getClassLoader()
          .getResourceAsStream(ParserConsts.PROPS_FILE);
      if (inputStream != null) {
        props.load(inputStream);
        properties = props.entrySet().stream().collect(
            Collectors.toMap(
                e -> e.getKey().toString(),
                e -> e.getValue().toString()
            )
        );
      } else {
        throw new FileNotFoundException("property file '"
            + ParserConsts.PROPS_FILE + "' not found in the classpath");
      }
    } catch(Exception e){
      LOG.error(e.getMessage());
    }

  }

  private static boolean createAuditTable(String dbName) {

    try(Connection connection = getConnection(dbName);
        Statement st = connection.createStatement()) {

      st.executeUpdate(properties.get(ParserConsts.CREATE_AUDIT_TABLE));
    } catch (SQLException e) {
      LOG.error(e.getMessage());
      return false;
    }
    return true;
  }

  private static boolean insertAudits(String dbName, String logs) {

    try(Connection connection = getConnection(dbName);
        PreparedStatement preparedStatement = connection.prepareStatement(
            properties.get(ParserConsts.INSERT_AUDITS))) {

      ArrayList<AuditEntry> auditEntries = parseAuditLogs(logs);

      final int batchSize = 1000;
      int count = 0;

      //Insert list to db
      for(AuditEntry audit : auditEntries) {
        preparedStatement.setString(1, audit.getTimestamp());
        preparedStatement.setString(2, audit.getLevel());
        preparedStatement.setString(3, audit.getLogger());
        preparedStatement.setString(4, audit.getUser());
        preparedStatement.setString(5, audit.getIp());
        preparedStatement.setString(6, audit.getOp());
        preparedStatement.setString(7, audit.getParams());
        preparedStatement.setString(8, audit.getResult());
        preparedStatement.setString(9, audit.getException());

        preparedStatement.addBatch();

        if(++count % batchSize == 0) {
          preparedStatement.executeBatch();
        }
      }
      if(auditEntries.size() > 0) {
        preparedStatement.executeBatch(); // insert remaining records
      }
    } catch (Exception e) {
      LOG.error(e.getMessage());
      return false;
    }
    return true;
  }

  private static ArrayList<AuditEntry> parseAuditLogs(String filePath)
      throws Exception {
    ArrayList<AuditEntry> listResult = new ArrayList<AuditEntry>();
    try(FileInputStream fis = new FileInputStream(filePath);
        InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
        BufferedReader bReader = new BufferedReader(isr)) {
      String currentLine = null;
      String[] entry = null;
      AuditEntry tempEntry = null;
      String nextLine = null;
      currentLine = bReader.readLine();
      nextLine = bReader.readLine();

      while(true) {
        if(tempEntry == null){
          tempEntry = new AuditEntry();
        }

        if(currentLine == null) {
          break;
        } else {
          if(!currentLine.matches(ParserConsts.DATE_REGEX)){
            tempEntry.appendException(currentLine);
          } else {
            entry = StringUtils.stripAll(currentLine.split("\\|"));
            String[] ops =
                entry[5].substring(entry[5].indexOf('=') + 1).split(" ", 2);
            tempEntry = new AuditEntry.Builder()
                .setTimestamp(entry[0])
                .setLevel(entry[1])
                .setLogger(entry[2])
                .setUser(entry[3].substring(entry[3].indexOf('=') + 1))
                .setIp(entry[4].substring(entry[4].indexOf('=') + 1))
                .setOp(ops[0])
                .setParams(ops[1])
                .setResult(entry[6].substring(entry[6].indexOf('=') + 1))
                .build();
            if(entry.length == 8){
              tempEntry.setException(entry[7]);
            }
          }
          if(nextLine == null || nextLine.matches(ParserConsts.DATE_REGEX)){
            listResult.add(tempEntry);
            tempEntry = null;
          }
          currentLine = nextLine;
          nextLine = bReader.readLine();
        }
      }
    } catch (RuntimeException rx) {
      throw rx;
    } catch (Exception ex) {
      throw ex;
    }

    return listResult;
  }

  public static String executeCustomQuery(String dbName, String query)
      throws SQLException {
    return executeStatement(dbName, query);
  }

  public static String executeTemplate(String dbName, String template)
      throws SQLException {
    return executeStatement(dbName,
        properties.get(template));
  }

  private static String executeStatement(String dbName, String sql)
      throws SQLException {
    StringBuilder result = new StringBuilder();
    ResultSet rs = null;
    Statement st = null;
    ResultSetMetaData rsm = null;
    try(Connection connection = getConnection(dbName)) {
      //loadProperties();

      if(connection != null){
        st = connection.createStatement();
        rs = st.executeQuery(sql);
        if(rs != null) {
          rsm = rs.getMetaData();
          int cols = rsm.getColumnCount();
          while(rs.next()){
            for(int index =1; index<=cols; index++){
              result.append(rs.getObject(index) + "\t");
            }
            result.append("\n");
          }
        }
        st.close();
        rs.close();
      }
    }
    return result.toString();
  }

  public static boolean validateTemplate(String templateName) {
    return (properties.get(templateName) != null);
  }
}
