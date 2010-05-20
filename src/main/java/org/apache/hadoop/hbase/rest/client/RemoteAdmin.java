/*
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.rest.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.rest.Constants;
import org.apache.hadoop.hbase.rest.model.TableSchemaModel;
import org.apache.hadoop.hbase.util.Bytes;

public class RemoteAdmin {

  final Client client;
  final Configuration conf;
  final String accessToken;
  final int maxRetries;
  final long sleepTime;

  /**
   * Constructor
   * @param client
   * @param conf
   */
  public RemoteAdmin(Client client, Configuration conf) {
    this(client, conf, null);
  }

  /**
   * Constructor
   * @param client
   * @param conf
   * @param accessToken
   */
  public RemoteAdmin(Client client, Configuration conf, String accessToken) {
    this.client = client;
    this.conf = conf;
    this.accessToken = accessToken;
    this.maxRetries = conf.getInt("hbase.rest.client.max.retries", 10);
    this.sleepTime = conf.getLong("hbase.rest.client.sleep", 1000);
  }

  /**
   * @param tableName name of table to check
   * @return true if all regions of the table are available
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableAvailable(String tableName) throws IOException {
    return isTableAvailable(Bytes.toBytes(tableName));
  }

  /**
   * @param tableName name of table to check
   * @return true if all regions of the table are available
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableAvailable(byte[] tableName) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append('/');
    if (accessToken != null) {
      sb.append(accessToken);
      sb.append('/');
    }
    sb.append(Bytes.toStringBinary(tableName));
    sb.append('/');
    sb.append("exists");
    int code = 0;
    for (int i = 0; i < maxRetries; i++) {
      Response response = client.get(sb.toString());
      code = response.getCode();
      switch (code) {
      case 200:
        return true;
      case 404:
        return false;
      case 509:
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) { }
        break;
      default:
        throw new IOException("exists request returned " + code);
      }
    }
    throw new IOException("exists request timed out");
  }

  /**
   * Creates a new table.
   * @param desc table descriptor for table
   * @throws IOException if a remote or network exception occurs
   */
  public void createTable(HTableDescriptor desc)
      throws IOException {
    TableSchemaModel model = new TableSchemaModel(desc);
    StringBuilder sb = new StringBuilder();
    sb.append('/');
    if (accessToken != null) {
      sb.append(accessToken);
      sb.append('/');
    }
    sb.append(Bytes.toStringBinary(desc.getName()));
    sb.append('/');
    sb.append("schema");
    int code = 0;
    for (int i = 0; i < maxRetries; i++) {
      Response response = client.put(sb.toString(), Constants.MIMETYPE_PROTOBUF,
        model.createProtobufOutput());
      code = response.getCode();
      switch (code) {
      case 201:
        return;
      case 509:
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) { }
        break;
      default:
        throw new IOException("create request returned " + code);
      }
    }
    throw new IOException("create request timed out");
  }

  /**
   * Deletes a table.
   * @param tableName name of table to delete
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteTable(final String tableName) throws IOException {
    deleteTable(Bytes.toBytes(tableName));
  }

  /**
   * Deletes a table.
   * @param tableName name of table to delete
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteTable(final byte [] tableName) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append('/');
    if (accessToken != null) {
      sb.append(accessToken);
      sb.append('/');
    }
    sb.append(Bytes.toStringBinary(tableName));
    sb.append('/');
    sb.append("schema");
    int code = 0;
    for (int i = 0; i < maxRetries; i++) { 
      Response response = client.delete(sb.toString());
      code = response.getCode();
      switch (code) {
      case 200:
        return;
      case 509:
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) { }
        break;
      default:
        throw new IOException("delete request returned " + code);
      }
    }
    throw new IOException("delete request timed out");
  }

}
