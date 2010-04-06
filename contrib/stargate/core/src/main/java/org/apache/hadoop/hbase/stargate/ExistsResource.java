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

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.hadoop.hbase.client.HBaseAdmin;

public class ExistsResource implements Constants {

  User user;
  String tableName;
  String actualTableName;
  CacheControl cacheControl;
  RESTServlet servlet;

  public ExistsResource(User user, String table) throws IOException {
    if (user != null) {
      this.user = user;
      this.actualTableName = 
        !user.isAdmin() ? (user.getName() + "." + table) : table;
    } else {
      this.actualTableName = table;
    }
    this.tableName = table;
    servlet = RESTServlet.getInstance();
    cacheControl = new CacheControl();
    cacheControl.setNoCache(true);
    cacheControl.setNoTransform(false);
  }

  @GET
  @Produces({MIMETYPE_TEXT, MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF,
    MIMETYPE_BINARY})
  public Response get(final @Context UriInfo uriInfo) throws IOException {
    if (!servlet.userRequestLimit(user, 1)) {
      return Response.status(509).build();
    }
    try {
      HBaseAdmin admin = new HBaseAdmin(servlet.getConfiguration());
      if (!admin.tableExists(actualTableName)) {
        throw new WebApplicationException(Response.Status.NOT_FOUND);
      }
    } catch (IOException e) {
      throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
    }
    ResponseBuilder response = Response.ok();
    response.cacheControl(cacheControl);
    return response.build();
  }

}
