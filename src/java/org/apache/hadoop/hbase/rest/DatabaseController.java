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
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;
import org.apache.hadoop.hbase.rest.parser.IHBaseRestParser;

public class DatabaseController extends AbstractController {

  @SuppressWarnings("unused")
  private Log LOG = LogFactory.getLog(DatabaseController.class);

  protected DatabaseModel getModel() {
    return (DatabaseModel) model;
  }

  @Override
  protected AbstractModel generateModel(HBaseConfiguration conf,
      HBaseAdmin admin) {
    return new DatabaseModel(conf, admin);
  }

  @Override
  public void get(Status s, byte[][] pathSegments,
      Map<String, String[]> queryMap) throws HBaseRestException {
    s.setNoQueryResults();
    DatabaseModel innerModel = getModel();

    if (queryMap.size() == 0) {
      s.setOK(innerModel.getDatabaseMetadata());
    } else {
      s.setBadRequest("Unknown query.");
    }
    s.respond();
  }

  @Override
  public void post(Status s, byte[][] pathSegments,
      Map<String, String[]> queryMap, byte[] input, IHBaseRestParser parser)
      throws HBaseRestException {
    s.setMethodNotImplemented();
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
    s.setMethodNotImplemented();
    s.respond();
  }
}
