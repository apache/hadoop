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

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.rest.descriptors.RowUpdateDescriptor;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;
import org.apache.hadoop.hbase.rest.parser.IHBaseRestParser;
import org.apache.hadoop.hbase.util.Bytes;

public class RowController extends AbstractController {

  @SuppressWarnings("unused")
  private Log LOG = LogFactory.getLog(RowController.class);

  protected RowModel getModel() {
    return (RowModel) model;
  }

  @Override
  protected AbstractModel generateModel(
      @SuppressWarnings("hiding") HBaseConfiguration conf, HBaseAdmin admin) {
    return new RowModel(conf, admin);
  }

  @Override
  public void get(Status s, byte[][] pathSegments,
      Map<String, String[]> queryMap) throws HBaseRestException {
    RowModel innerModel = getModel();
    s.setNoQueryResults();

    byte[] tableName;
    byte[] rowName;

    tableName = pathSegments[0];
    rowName = pathSegments[2];
    RowResult row = null;
    
    if (queryMap.size() == 0 && pathSegments.length <= 3) {
      row = innerModel.get(tableName, rowName);
    } else if (pathSegments.length == 4
        && Bytes.toString(pathSegments[3]).toLowerCase().equals(
            RESTConstants.TIME_STAMPS)) {
      innerModel.getTimestamps(tableName, rowName);
    } else {
      row = innerModel.get(tableName, rowName, this.getColumnsFromQueryMap(queryMap));
    }
    if(row == null) {
      throw new HBaseRestException("row not found");
    }
    s.setOK(row);
    s.respond();
  }

  @Override
  public void post(Status s, byte[][] pathSegments,
      Map<String, String[]> queryMap, byte[] input, IHBaseRestParser parser)
      throws HBaseRestException {
    RowModel innerModel = getModel();

    BatchUpdate b;
    RowUpdateDescriptor rud = parser
        .getRowUpdateDescriptor(input, pathSegments);

    if (input.length == 0) {
      s.setUnsupportedMediaType("no data send with post request");
      s.respond();
      return;
    }

    b = new BatchUpdate(rud.getRowName());

    for (byte[] key : rud.getColVals().keySet()) {
      b.put(key, rud.getColVals().get(key));
    }

    try {
      innerModel.post(rud.getTableName().getBytes(), b);
      s.setOK();
    } catch (HBaseRestException e) {
      s.setUnsupportedMediaType(e.getMessage());
    }
    s.respond();
  }

  @Override
  public void put(Status s, byte[][] pathSegments,
      Map<String, String[]> queryMap, byte[] input, IHBaseRestParser parser)
      throws HBaseRestException {
    s.setMethodNotImplemented();
    s.respond();
  }

  @Override
  public void delete(Status s, byte[][] pathSegments,
      Map<String, String[]> queryMap) throws HBaseRestException {
    RowModel innerModel = getModel();
    byte[] tableName;
    byte[] rowName;

    tableName = pathSegments[0];
    rowName = pathSegments[2];
    if(queryMap.size() == 0) {
      innerModel.delete(tableName, rowName);
    } else {
      innerModel.delete(tableName, rowName, this.getColumnsFromQueryMap(queryMap));
    }
    s.setOK();
    s.respond();
  }
}
