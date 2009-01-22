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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;
import org.apache.hadoop.hbase.rest.parser.IHBaseRestParser;
import org.apache.hadoop.hbase.util.Bytes;

public abstract class AbstractController implements RESTConstants {

  private Log LOG = LogFactory.getLog(AbstractController.class);
  protected Configuration conf;
  protected AbstractModel model;

  public void initialize(HBaseConfiguration conf, HBaseAdmin admin) {
    this.conf = conf;
    this.model = generateModel(conf, admin);
  }

  public abstract void get(Status s, byte[][] pathSegments,
      Map<String, String[]> queryMap) throws HBaseRestException;

  public abstract void post(Status s, byte[][] pathSegments,
      Map<String, String[]> queryMap, byte[] input, IHBaseRestParser parser)
      throws HBaseRestException;

  public abstract void put(Status s, byte[][] pathSegments,
      Map<String, String[]> queryMap, byte[] input, IHBaseRestParser parser)
      throws HBaseRestException;

  public abstract void delete(Status s, byte[][] pathSegments,
      Map<String, String[]> queryMap) throws HBaseRestException;

  protected abstract AbstractModel generateModel(HBaseConfiguration conf,
      HBaseAdmin a);
  
  protected byte[][] getColumnsFromQueryMap(Map<String, String[]> queryMap) {
    byte[][] columns = null;
    String[] columnArray = queryMap.get(RESTConstants.COLUMN);
    if (columnArray != null) {
      columns = new byte[columnArray.length][];
      for (int i = 0; i < columnArray.length; i++) {
        columns[i] = Bytes.toBytes(columnArray[i]);
      }
    }
    return columns;
  }
}
