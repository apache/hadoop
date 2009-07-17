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
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.stargate.model.CellModel;
import org.apache.hadoop.hbase.stargate.model.CellSetModel;
import org.apache.hadoop.hbase.stargate.model.RowModel;
import org.apache.hadoop.hbase.util.Bytes;

public class RowResource implements Constants {
  private static final Log LOG = LogFactory.getLog(RowResource.class);

  private String table;
  private RowSpec rowspec;
  private CacheControl cacheControl;

  public RowResource(String table, String rowspec, String versions) 
      throws IOException {
    this.table = table;
    this.rowspec = new RowSpec(rowspec);
    if (versions != null) {
      this.rowspec.setMaxVersions(Integer.valueOf(versions));
    }
    cacheControl = new CacheControl();
    cacheControl.setMaxAge(RESTServlet.getInstance().getMaxAge(table));
    cacheControl.setNoTransform(false);
  }

  @GET
  @Produces({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_JAVASCRIPT,
    MIMETYPE_PROTOBUF})
  public Response get(@Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("GET " + uriInfo.getAbsolutePath());
    }
    try {
      ResultGenerator generator = ResultGenerator.fromRowSpec(table, rowspec);
      if (!generator.hasNext()) {
        throw new WebApplicationException(Response.Status.NOT_FOUND);
      }
      CellSetModel model = new CellSetModel();
      KeyValue value = generator.next();
      byte[] rowKey = value.getRow();
      RowModel rowModel = new RowModel(rowKey);
      do {
        if (!Bytes.equals(value.getRow(), rowKey)) {
          model.addRow(rowModel);
          rowKey = value.getRow();
          rowModel = new RowModel(rowKey);
        }
        rowModel.addCell(
          new CellModel(value.getColumn(), value.getTimestamp(),
              value.getValue()));
        value = generator.next();
      } while (value != null);
      model.addRow(rowModel);
      ResponseBuilder response = Response.ok(model);
      response.cacheControl(cacheControl);
      return response.build();
    } catch (IOException e) {
      throw new WebApplicationException(e,
                  Response.Status.SERVICE_UNAVAILABLE);
    }
  }

  @GET
  @Produces(MIMETYPE_BINARY)
  public Response getBinary(@Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("GET " + uriInfo.getAbsolutePath() + " as "+ MIMETYPE_BINARY);
    }
    // doesn't make sense to use a non specific coordinate as this can only
    // return a single cell
    if (!rowspec.hasColumns() || rowspec.getColumns().length > 1) {
      throw new WebApplicationException(Response.Status.BAD_REQUEST);
    }
    try {
      ResultGenerator generator = ResultGenerator.fromRowSpec(table, rowspec);
      if (!generator.hasNext()) {
        throw new WebApplicationException(Response.Status.NOT_FOUND);
      }
      KeyValue value = generator.next();
      ResponseBuilder response = Response.ok(value.getValue());
      response.cacheControl(cacheControl);
      response.header("X-Timestamp", value.getTimestamp());
      return response.build();
    } catch (IOException e) {
      throw new WebApplicationException(e,
                  Response.Status.SERVICE_UNAVAILABLE);
    }
  }

  private Response update(CellSetModel model, boolean replace) {
    HTablePool pool;
    try {
      pool = RESTServlet.getInstance().getTablePool();
    } catch (IOException e) {
      throw new WebApplicationException(e, 
                  Response.Status.INTERNAL_SERVER_ERROR);
    }
    HTable table = null;
    try {
      table = pool.getTable(this.table);
      for (RowModel row: model.getRows()) {
        Put put = new Put(row.getKey());
        for (CellModel cell: row.getCells()) {
          byte [][] parts = KeyValue.parseColumn(cell.getColumn());
          if (cell.hasUserTimestamp()) {
            put.add(parts[0], parts[1], cell.getTimestamp(), cell.getValue());
          } else {
            put.add(parts[0], parts[1], cell.getValue());
          }
        }
        table.put(put);
        if (LOG.isDebugEnabled()) {
          LOG.debug("PUT " + put.toString());
        }
      }
      table.flushCommits();
      ResponseBuilder response = Response.ok();
      return response.build();
    } catch (IOException e) {
      throw new WebApplicationException(e,
                  Response.Status.SERVICE_UNAVAILABLE);
    } finally {
      if (table != null) {
        pool.putTable(table);
      }
    }
  }

  private Response updateBinary(byte[] message, HttpHeaders headers, 
      boolean replace) {
    HTablePool pool;
    try {
      pool = RESTServlet.getInstance().getTablePool();
    } catch (IOException e) {
      throw new WebApplicationException(e, 
                  Response.Status.INTERNAL_SERVER_ERROR);
    }
    HTable table = null;    
    try {
      byte[] row = rowspec.getRow();
      byte[][] columns = rowspec.getColumns();
      byte[] column = null;
      if (columns != null) {
        column = columns[0];
      }
      long timestamp = -1;
      List<String> vals = headers.getRequestHeader("X-Row");
      if (vals != null && !vals.isEmpty()) {
        row = Bytes.toBytes(vals.get(0));
      }
      vals = headers.getRequestHeader("X-Column");
      if (vals != null && !vals.isEmpty()) {
        column = Bytes.toBytes(vals.get(0));
      }
      vals = headers.getRequestHeader("X-Timestamp");
      if (vals != null && !vals.isEmpty()) {
        timestamp = Long.valueOf(vals.get(0));
      }
      if (column == null) {
        throw new WebApplicationException(Response.Status.BAD_REQUEST);
      }
      Put put = new Put(row);
      byte parts[][] = KeyValue.parseColumn(column);
      if (timestamp >= 0) {
        put.add(parts[0], parts[1], timestamp, message);
      } else {
        put.add(parts[0], parts[1], message);
      }
      table = pool.getTable(this.table);
      table.put(put);
      if (LOG.isDebugEnabled()) {
        LOG.debug("PUT " + put.toString());
      }
      table.flushCommits();
      return Response.ok().build();
    } catch (IOException e) {
      throw new WebApplicationException(e,
                  Response.Status.SERVICE_UNAVAILABLE);
    } finally {
      if (table != null) {
        pool.putTable(table);
      }
    }
  }

  @PUT
  @Consumes({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_JAVASCRIPT,
    MIMETYPE_PROTOBUF})
  public Response put(CellSetModel model, @Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("PUT " + uriInfo.getAbsolutePath());
    }
    return update(model, true);
  }

  @PUT
  @Consumes(MIMETYPE_BINARY)
  public Response putBinary(byte[] message, @Context UriInfo uriInfo, 
      @Context HttpHeaders headers)
  {
    if (LOG.isDebugEnabled()) {
      LOG.debug("PUT " + uriInfo.getAbsolutePath() + " as "+ MIMETYPE_BINARY);
    }
    return updateBinary(message, headers, true);
  }

  @POST
  @Consumes({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_JAVASCRIPT,
    MIMETYPE_PROTOBUF})
  public Response post(CellSetModel model, @Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("POST " + uriInfo.getAbsolutePath());
    }
    return update(model, false);
  }

  @POST
  @Consumes(MIMETYPE_BINARY)
  public Response postBinary(byte[] message, @Context UriInfo uriInfo, 
      @Context HttpHeaders headers)
  {
    if (LOG.isDebugEnabled()) {
      LOG.debug("POST " + uriInfo.getAbsolutePath() + " as "+ MIMETYPE_BINARY);
    }
    return updateBinary(message, headers, false);
  }

  @DELETE
  public Response delete(@Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("DELETE " + uriInfo.getAbsolutePath());
    }
    Delete delete = new Delete(rowspec.getRow());
    for (byte[] column: rowspec.getColumns()) {
      byte[][] split = KeyValue.parseColumn(column);
      if (rowspec.hasTimestamp()) {
        delete.deleteColumns(split[0], split[1], rowspec.getTimestamp());
      } else {
        delete.deleteColumns(split[0], split[1]);        
      }
    }
    HTablePool pool;
    try {
      pool = RESTServlet.getInstance().getTablePool();
    } catch (IOException e) {
      throw new WebApplicationException(e, 
                  Response.Status.INTERNAL_SERVER_ERROR);
    }
    HTable table = null;
    try {
      table = pool.getTable(this.table);
      table.delete(delete);
      if (LOG.isDebugEnabled()) {
        LOG.debug("DELETE " + delete.toString());
      }
      table.flushCommits();
    } catch (IOException e) {
      throw new WebApplicationException(e, 
                  Response.Status.SERVICE_UNAVAILABLE);
    } finally {
      if (table != null) {
        pool.putTable(table);
      }
    }
    return Response.ok().build();
  }
}
