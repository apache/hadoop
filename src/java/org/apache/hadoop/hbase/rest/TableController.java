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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;
import org.apache.hadoop.hbase.rest.parser.IHBaseRestParser;
import org.apache.hadoop.hbase.util.Bytes;

public class TableController extends AbstractController {

  @SuppressWarnings("unused")
  private Log LOG = LogFactory.getLog(TableController.class);

  protected TableModel getModel() {
    return (TableModel) model;
  }

  @Override
  protected AbstractModel generateModel(
      @SuppressWarnings("hiding") HBaseConfiguration conf, HBaseAdmin admin) {
    return new TableModel(conf, admin);
  }

  @Override
  public void get(Status s, byte[][] pathSegments,
      Map<String, String[]> queryMap) throws HBaseRestException {
    TableModel innerModel = getModel();

    byte[] tableName;

    tableName = pathSegments[0];
    if (pathSegments.length < 2) {
      s.setOK(innerModel.getTableMetadata(Bytes.toString(tableName)));
    } else {
      if (Bytes.toString(pathSegments[1]).toLowerCase().equals(REGIONS)) {
        s.setOK(innerModel.getTableRegions(Bytes.toString(tableName)));
      } else {
        s.setBadRequest("unknown query.");
      }
    }
    s.respond();
  }

  /*
   * (non-Javadoc)
   * 
   * @param input column descriptor JSON. Should be of the form: <pre>
   * {"column_families":[ { "name":STRING, "bloomfilter":BOOLEAN,
   * "max_versions":INTEGER, "compression_type":STRING, "in_memory":BOOLEAN,
   * "block_cache_enabled":BOOLEAN, "max_value_length":INTEGER,
   * "time_to_live":INTEGER ]} </pre> If any of the json object fields (except
   * name) are not included the default values will be included instead. The
   * default values are: <pre> bloomfilter => false max_versions => 3
   * compression_type => NONE in_memory => false block_cache_enabled => false
   * max_value_length => 2147483647 time_to_live => Integer.MAX_VALUE </pre>
   * 
   * @see
   * org.apache.hadoop.hbase.rest.AbstractController#post(org.apache.hadoop.
   * hbase.rest.Status, byte[][], java.util.Map, byte[],
   * org.apache.hadoop.hbase.rest.parser.IHBaseRestParser)
   */
  @Override
  public void post(Status s, byte[][] pathSegments,
      Map<String, String[]> queryMap, byte[] input, IHBaseRestParser parser)
      throws HBaseRestException {
    TableModel innerModel = getModel();

    byte[] tableName;

    if (pathSegments.length == 0) {
      // If no input, we don't know columnfamily schema, so send
      // no data
      if (input.length == 0) {
        s.setBadRequest("no data send with post request");
      } else {
        HTableDescriptor htd = parser.getTableDescriptor(input);
        // Send to innerModel. If iM returns false, means the
        // table already exists so return conflict.
        if (!innerModel.post(htd.getName(), htd)) {
          s.setConflict("table already exists");
        } else {
          // Otherwise successfully created table. Return "created":true
          s.setCreated();
        }
      }
    } else if (Bytes.toString(pathSegments[1]).toLowerCase().equals(
        RESTConstants.ENABLE)) {
      tableName = pathSegments[0];
      innerModel.enableTable(tableName);
      s.setAccepted();
    } else if (Bytes.toString(pathSegments[1]).toLowerCase().equals(
        RESTConstants.DISABLE)) {
      tableName = pathSegments[0];
      innerModel.disableTable(tableName);
      s.setAccepted();
    } else {
      s.setBadRequest("Unknown Query.");
    }
    s.respond();
  }

  @Override
  public void put(Status s, byte[][] pathSegments,
      Map<String, String[]> queryMap, byte[] input, IHBaseRestParser parser)
      throws HBaseRestException {
    if (pathSegments.length != 1) {
      s.setBadRequest("must specifify the name of the table");
      s.respond();
    } else if (queryMap.size() > 0) {
      s
          .setBadRequest("no query string should be specified when updating a table");
      s.respond();
    } else {
      ArrayList<HColumnDescriptor> newColumns = parser
          .getColumnDescriptors(input);
      byte[] tableName = pathSegments[0];
      getModel().updateTable(Bytes.toString(tableName), newColumns);
      s.setOK();
      s.respond();
    }
  }

  @Override
  public void delete(Status s, byte[][] pathSegments,
      Map<String, String[]> queryMap) throws HBaseRestException {
    TableModel innerModel = getModel();

    byte[] tableName;

    tableName = pathSegments[0];

    if (pathSegments.length == 1) {
      if (!innerModel.delete(tableName)) {
        s.setBadRequest("table does not exist");
      } else {
        s.setAccepted();
      }
      s.respond();
    } else {

    }
  }

}