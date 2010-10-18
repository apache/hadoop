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

package org.apache.hadoop.hbase.rest;

import java.io.IOException;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.Response.ResponseBuilder;

import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.rest.model.ColumnSchemaModel;
import org.apache.hadoop.hbase.rest.model.TableSchemaModel;
import org.apache.hadoop.hbase.util.Bytes;

public class SchemaResource extends ResourceBase {
  private static final Log LOG = LogFactory.getLog(SchemaResource.class);

  static CacheControl cacheControl;
  static {
    cacheControl = new CacheControl();
    cacheControl.setNoCache(true);
    cacheControl.setNoTransform(false);
  }

  String tableName;

  /**
   * Constructor
   * @param table
   * @throws IOException
   */
  public SchemaResource(String table) throws IOException {
    super();
    this.tableName = table;
  }

  private HTableDescriptor getTableSchema() throws IOException,
      TableNotFoundException {
    HTablePool pool = servlet.getTablePool();
    HTableInterface table = pool.getTable(tableName);
    try {
      return table.getTableDescriptor();
    } finally {
      pool.putTable(table);
    }
  }

  @GET
  @Produces({MIMETYPE_TEXT, MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF})
  public Response get(final @Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("GET " + uriInfo.getAbsolutePath());
    }
    servlet.getMetrics().incrementRequests(1);
    try {
      ResponseBuilder response =
        Response.ok(new TableSchemaModel(getTableSchema()));
      response.cacheControl(cacheControl);
      return response.build();
    } catch (TableNotFoundException e) {
      throw new WebApplicationException(Response.Status.NOT_FOUND);
    } catch (IOException e) {
      throw new WebApplicationException(e,
                  Response.Status.SERVICE_UNAVAILABLE);
    }
  }

  private Response replace(final byte[] name, final TableSchemaModel model,
      final UriInfo uriInfo, final HBaseAdmin admin) {
    try {
      HTableDescriptor htd = new HTableDescriptor(name);
      for (Map.Entry<QName,Object> e: model.getAny().entrySet()) {
        htd.setValue(e.getKey().getLocalPart(), e.getValue().toString());
      }
      for (ColumnSchemaModel family: model.getColumns()) {
        HColumnDescriptor hcd = new HColumnDescriptor(family.getName());
        for (Map.Entry<QName,Object> e: family.getAny().entrySet()) {
          hcd.setValue(e.getKey().getLocalPart(), e.getValue().toString());
        }
        htd.addFamily(hcd);
      }
      if (admin.tableExists(name)) {
        admin.disableTable(name);
        admin.modifyTable(name, htd);
        admin.enableTable(name);
      } else try {
        admin.createTable(htd);
      } catch (TableExistsException e) {
        // race, someone else created a table with the same name
        throw new WebApplicationException(e, Response.Status.NOT_MODIFIED);
      }
      return Response.created(uriInfo.getAbsolutePath()).build();
    } catch (IOException e) {
      throw new WebApplicationException(e, 
            Response.Status.SERVICE_UNAVAILABLE);
    }      
  } 

  private Response update(final byte[] name, final TableSchemaModel model,
      final UriInfo uriInfo, final HBaseAdmin admin) {
    try {
      HTableDescriptor htd = admin.getTableDescriptor(name);
      admin.disableTable(name);
      try {
        for (ColumnSchemaModel family: model.getColumns()) {
          HColumnDescriptor hcd = new HColumnDescriptor(family.getName());
          for (Map.Entry<QName,Object> e: family.getAny().entrySet()) {
            hcd.setValue(e.getKey().getLocalPart(), e.getValue().toString());
          }
          if (htd.hasFamily(hcd.getName())) {
            admin.modifyColumn(name, hcd);
          } else {
            admin.addColumn(model.getName(), hcd);            
          }
        }
      } catch (IOException e) {
        throw new WebApplicationException(e, 
            Response.Status.INTERNAL_SERVER_ERROR);
      } finally {
        admin.enableTable(tableName);
      }
      return Response.ok().build();
    } catch (IOException e) {
      throw new WebApplicationException(e,
          Response.Status.SERVICE_UNAVAILABLE);
    }
  }

  private Response update(final TableSchemaModel model, final boolean replace,
      final UriInfo uriInfo) {
    try {
      byte[] name = Bytes.toBytes(tableName);
      HBaseAdmin admin = new HBaseAdmin(servlet.getConfiguration());
      if (replace || !admin.tableExists(name)) {
        return replace(name, model, uriInfo, admin);
      } else {
        return update(name, model, uriInfo, admin);
      }
    } catch (IOException e) {
      throw new WebApplicationException(e, 
            Response.Status.SERVICE_UNAVAILABLE);
    }
  }

  @PUT
  @Consumes({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF})
  public Response put(final TableSchemaModel model, 
      final @Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("PUT " + uriInfo.getAbsolutePath());
    }
    servlet.getMetrics().incrementRequests(1);
    return update(model, true, uriInfo);
  }

  @POST
  @Consumes({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF})
  public Response post(final TableSchemaModel model, 
      final @Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("PUT " + uriInfo.getAbsolutePath());
    }
    servlet.getMetrics().incrementRequests(1);
    return update(model, false, uriInfo);
  }

  @DELETE
  public Response delete(final @Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("DELETE " + uriInfo.getAbsolutePath());
    }
    servlet.getMetrics().incrementRequests(1);
    try {
      HBaseAdmin admin = new HBaseAdmin(servlet.getConfiguration());
      boolean success = false;
      for (int i = 0; i < 10; i++) try {
        admin.disableTable(tableName);
        success = true;
        break;
      } catch (IOException e) {
      }
      if (!success) {
        throw new IOException("could not disable table");
      }
      admin.deleteTable(tableName);
      return Response.ok().build();
    } catch (TableNotFoundException e) {
      throw new WebApplicationException(Response.Status.NOT_FOUND);
    } catch (IOException e) {
      throw new WebApplicationException(e, 
            Response.Status.SERVICE_UNAVAILABLE);
    }
  }
}
