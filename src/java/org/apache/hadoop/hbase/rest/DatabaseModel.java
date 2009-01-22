/**
 * Copyright 2008 The Apache Software Foundation
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
package org.apache.hadoop.hbase.rest;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;
import org.apache.hadoop.hbase.rest.serializer.IRestSerializer;
import org.apache.hadoop.hbase.rest.serializer.ISerializable;

import agilejson.TOJSON;

public class DatabaseModel extends AbstractModel {

  @SuppressWarnings("unused")
  private Log LOG = LogFactory.getLog(DatabaseModel.class);

  public DatabaseModel(HBaseConfiguration conf, HBaseAdmin admin) {
    super.initialize(conf, admin);
  }

  public static class DatabaseMetadata implements ISerializable {
    protected boolean master_running;
    protected HTableDescriptor[] tables;

    public DatabaseMetadata(HBaseAdmin a) throws IOException {
      master_running = a.isMasterRunning();
      tables = a.listTables();
    }

    @TOJSON(prefixLength = 2)
    public boolean isMasterRunning() {
      return master_running;
    }

    @TOJSON
    public HTableDescriptor[] getTables() {
      return tables;
    }

    public void restSerialize(IRestSerializer serializer)
        throws HBaseRestException {
      serializer.serializeDatabaseMetadata(this);
    }
  }

  // Serialize admin ourselves to json object
  // rather than returning the admin object for obvious reasons
  public DatabaseMetadata getMetadata() throws HBaseRestException {
    return getDatabaseMetadata();
  }

  protected DatabaseMetadata getDatabaseMetadata() throws HBaseRestException {
    DatabaseMetadata databaseMetadata = null;
    try {
      databaseMetadata = new DatabaseMetadata(this.admin);
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }

    return databaseMetadata;
  }
}
