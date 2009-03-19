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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;
import org.apache.hadoop.hbase.rest.parser.IHBaseRestParser;
import org.apache.hadoop.hbase.util.Bytes;

public class TimestampController extends AbstractController {

  @SuppressWarnings("unused")
  private Log LOG = LogFactory.getLog(TimestampController.class);

  protected TimestampModel getModel() {
    return (TimestampModel) model;
  }

  @Override
  protected AbstractModel generateModel(
      HBaseConfiguration conf, HBaseAdmin admin) {
    return new TimestampModel(conf, admin);
  }

  @Override
  public void get(Status s, byte[][] pathSegments,
      Map<String, String[]> queryMap) throws HBaseRestException {
    TimestampModel innerModel = getModel();

    byte[] tableName;
    byte[] rowName;
    long timestamp;

    tableName = pathSegments[0];
    rowName = pathSegments[2];
    timestamp = Bytes.toLong(pathSegments[3]);

    if (queryMap.size() == 0) {
      s.setOK(innerModel.get(tableName, rowName, timestamp));
    } else {
      // get the column names if any were passed in
      String[] column_params = queryMap.get(RESTConstants.COLUMN);
      byte[][] columns = null;

      if (column_params != null && column_params.length > 0) {
        List<String> available_columns = new ArrayList<String>();
        for (String column_param : column_params) {
          available_columns.add(column_param);
        }
        columns = Bytes.toByteArrays(available_columns.toArray(new String[0]));
      }
      s.setOK(innerModel.get(tableName, rowName, columns, timestamp));
    }
    s.respond();
  }

  @Override
  public void post(Status s, byte[][] pathSegments,
      Map<String, String[]> queryMap, byte[] input, IHBaseRestParser parser)
      throws HBaseRestException {
    TimestampModel innerModel = getModel();

    byte[] tableName;
    byte[] rowName;
    byte[] columnName;
    long timestamp;

    tableName = pathSegments[0];
    rowName = pathSegments[1];
    columnName = pathSegments[2];
    timestamp = Bytes.toLong(pathSegments[3]);

    try {
      if (queryMap.size() == 0) {
        innerModel.post(tableName, rowName, columnName, timestamp, input);
        s.setOK();
      } else {
        s.setUnsupportedMediaType("Unknown Query.");
      }
    } catch (HBaseRestException e) {
      s.setUnsupportedMediaType(e.getMessage());
    }
    s.respond();
  }

  @Override
  public void put(Status s, byte[][] pathSegments,
      Map<String, String[]> queryMap, byte[] input, IHBaseRestParser parser)
      throws HBaseRestException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void delete(Status s, byte[][] pathSegments,
      Map<String, String[]> queryMap) throws HBaseRestException {
    TimestampModel innerModel = getModel();

    byte[] tableName;
    byte[] rowName;
    long timestamp;

    tableName = pathSegments[0];
    rowName = pathSegments[2];
    timestamp = Bytes.toLong(pathSegments[3]);

    if (queryMap.size() == 0) {
      innerModel.delete(tableName, rowName, timestamp);
    } else {
      innerModel.delete(tableName, rowName, this
          .getColumnsFromQueryMap(queryMap), timestamp);
    }
    s.setAccepted();
    s.respond();
  }
}
