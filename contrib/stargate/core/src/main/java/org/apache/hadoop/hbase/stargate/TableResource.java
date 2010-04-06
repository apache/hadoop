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

package org.apache.hadoop.hbase.stargate;

import java.io.IOException;

import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import org.apache.hadoop.hbase.stargate.User;

public class TableResource implements Constants {

  User user;
  String table;

  public TableResource(User user, String table) {
    this.user = user;
    this.table = table;
  }

  @Path("exists")
  public ExistsResource getExistsResource() throws IOException {
    return new ExistsResource(user, table);
  }

  @Path("regions")
  public RegionsResource getRegionsResource() throws IOException {
    return new RegionsResource(user, table);
  }

  @Path("scanner")
  public ScannerResource getScannerResource() throws IOException {
    return new ScannerResource(user, table);
  }

  @Path("schema")
  public SchemaResource getSchemaResource() throws IOException {
    return new SchemaResource(user, table);
  }

  @Path("{rowspec: .+}")
  public RowResource getRowResource(
      final @PathParam("rowspec") String rowspec,
      final @QueryParam("v") String versions) {
    try {
      return new RowResource(user, table, rowspec, versions);
    } catch (IOException e) {
      throw new WebApplicationException(e, 
                  Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

}
