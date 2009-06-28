/*
 * Copyright 2009 The Apache Software Foundation
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
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.stargate.model.TableListModel;
import org.apache.hadoop.hbase.stargate.model.TableModel;

@Path("/")
public class TableResource implements Constants {
  private static final Log LOG = LogFactory.getLog(TableResource.class);

  private CacheControl cacheControl;

  public TableResource() {
    cacheControl = new CacheControl();
    cacheControl.setNoCache(true);
    cacheControl.setNoTransform(false);
  }

  private HTableDescriptor[] getTableList() throws IOException {
    HBaseAdmin admin =
      new HBaseAdmin(RESTServlet.getInstance().getConfiguration());
    HTableDescriptor[] list = admin.listTables();
    if (LOG.isDebugEnabled()) {
      LOG.debug("getTableList:");
      for (HTableDescriptor htd: list) {
        LOG.debug(htd.toString());
      }
    }
    return list;
  }

  @GET
  @Produces({MIMETYPE_TEXT, MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_JAVASCRIPT,
    MIMETYPE_PROTOBUF})
  public Response get(@Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("GET " + uriInfo.getAbsolutePath());
    }
    try {
      TableListModel tableList = new TableListModel();
      for (HTableDescriptor htd: getTableList()) {
        if (htd.isMetaRegion()) {
          continue;
        }
        tableList.add(new TableModel(htd.getNameAsString()));
      }
      ResponseBuilder response = Response.ok(tableList);
      response.cacheControl(cacheControl);
      return response.build();
    } catch (IOException e) {
      throw new WebApplicationException(e, 
                  Response.Status.SERVICE_UNAVAILABLE);
    }
  }

  @Path("{table}/regions")
  public RegionsResource getRegionsResource(
      @PathParam("table") String table) {
    return new RegionsResource(table);    
  }

  @Path("{table}/scanner")
  public ScannerResource getScannerResource(
      @PathParam("table") String table) {
    return new ScannerResource(table);    
  }

  @Path("{table}/schema")
  public SchemaResource getSchemaResource(
      @PathParam("table") String table) {
    return new SchemaResource(table);    
  }

  @Path("{table}/{rowspec: .+}")
  public RowResource getRowResource(
      @PathParam("table") String table,
      @PathParam("rowspec") String rowspec,
      @QueryParam("v") String versions) {
    try {
      return new RowResource(table, rowspec, versions);
    } catch (IOException e) {
      throw new WebApplicationException(e, 
                  Response.Status.INTERNAL_SERVER_ERROR);
    }
  }
}
