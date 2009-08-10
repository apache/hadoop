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
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

public abstract class AbstractModel {

  @SuppressWarnings("unused")
  private Log LOG = LogFactory.getLog(AbstractModel.class);
  protected HBaseConfiguration conf;
  protected HBaseAdmin admin;

  protected static class Encodings {

    protected interface Encoding {

      String encode(byte[] b) throws HBaseRestException;
    }

    public static Encoding EBase64 = new Encoding() {

      public String encode(byte[] b) throws HBaseRestException {
        return Base64.encodeBytes(b);
      }
    };
    public static Encoding EUTF8 = new Encoding() {

      public String encode(byte[] b) throws HBaseRestException {
        return new String(b);
      }
    };
  }

  protected static final Encodings.Encoding encoding = Encodings.EUTF8;

  public void initialize(HBaseConfiguration conf, HBaseAdmin admin) {
    this.conf = conf;
    this.admin = admin;
  }

  protected byte[][] getColumns(byte[] tableName) throws HBaseRestException {
    try {
      HTable h = new HTable(this.conf, tableName);
      Collection<HColumnDescriptor> columns = h.getTableDescriptor()
          .getFamilies();
      byte[][] resultant = new byte[columns.size()][];
      int count = 0;

      for (HColumnDescriptor c : columns) {
        resultant[count++] = c.getNameWithColon();
      }

      return resultant;
    } catch (IOException e) {
      throw new HBaseRestException(e);
    }
  }

  protected static final byte COLON = Bytes.toBytes(":")[0];

  protected boolean isColumnFamily(byte[] columnName) {
    for (int i = 0; i < columnName.length; i++) {
      if (columnName[i] == COLON) {
        return true;
      }
    }
    return false;
  }
}