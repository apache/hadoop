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

package org.apache.hadoop.hbase.stargate.auth;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.stargate.Constants;
import org.apache.hadoop.hbase.stargate.User;
import org.apache.hadoop.hbase.util.Bytes;

public class HTableAuthenticator extends Authenticator implements Constants {

  static final byte[] USER = Bytes.toBytes("user");
  static final byte[] NAME = Bytes.toBytes("name");
  static final byte[] ADMIN = Bytes.toBytes("admin");
  static final byte[] DISABLED = Bytes.toBytes("disabled");

  Configuration conf;
  String tableName;
  HTable table;

  /**
   * Default constructor
   */
  public HTableAuthenticator() {
    this(HBaseConfiguration.create());
  }

  /**
   * Constructor
   * @param conf
   */
  public HTableAuthenticator(Configuration conf) {
    this(conf, conf.get("stargate.auth.htable.name", USERS_TABLE));
  }

  /**
   * Constructor
   * @param conf
   * @param tableName
   */
  public HTableAuthenticator(Configuration conf, String tableName) {
    this.conf = conf;
    this.tableName = tableName;
  }

  /**
   * Constructor
   * @param conf
   * @param table
   */
  public HTableAuthenticator(Configuration conf, HTable table) {
    this.conf = conf;
    this.table = table;
    this.tableName = Bytes.toString(table.getTableName());
  }

  @Override
  public User getUserForToken(String token) throws IOException {
    if (table == null) {
      this.table = new HTable(conf, tableName);
    }
    Get get = new Get(Bytes.toBytes(token));
    get.addColumn(USER, NAME);
    get.addColumn(USER, ADMIN);
    get.addColumn(USER, DISABLED);
    Result result = table.get(get);
    byte[] value = result.getValue(USER, NAME);
    if (value == null) {
      return null;
    }
    String name = Bytes.toString(value);
    boolean admin = false;
    value = result.getValue(USER, ADMIN);
    if (value != null) {
      admin = Bytes.toBoolean(value);
    }
    boolean disabled = false;
    value = result.getValue(USER, DISABLED);
    if (value != null) {
      disabled = Bytes.toBoolean(value);
    }
    return new User(name, token, admin, disabled);
  }

}
