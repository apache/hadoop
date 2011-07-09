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
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;
import org.apache.hadoop.hbase.rest.transform.Transform;
import org.apache.hadoop.hbase.util.Bytes;

public class RowResource extends ResourceBase {
  private static final Log LOG = LogFactory.getLog(RowResource.class);

  TableResource tableResource;
  RowSpec rowspec;

  /**
   * Constructor
   * @param tableResource
   * @param rowspec
   * @param versions
   * @throws IOException
   */
  public RowResource(TableResource tableResource, String rowspec,
      String versions) throws IOException {
    super();
    this.tableResource = tableResource;
    this.rowspec = new RowSpec(rowspec);
    if (versions != null) {
      this.rowspec.setMaxVersions(Integer.valueOf(versions));
    }
  }

  @GET
  @Produces({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF})
  public Response get(final @Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("GET " + uriInfo.getAbsolutePath());
    }
    servlet.getMetrics().incrementRequests(1);
    try {
      ResultGenerator generator =
        ResultGenerator.fromRowSpec(tableResource.getName(), rowspec, null);
      if (!generator.hasNext()) {
        throw new WebApplicationException(Response.Status.NOT_FOUND);
      }
      int count = 0;
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
        byte[] family = value.getFamily();
        byte[] qualifier = value.getQualifier();
        byte[] data = tableResource.transform(family, qualifier,
          value.getValue(), Transform.Direction.OUT);
        rowModel.addCell(new CellModel(family, qualifier,
          value.getTimestamp(), data));
        if (++count > rowspec.getMaxValues()) {
          break;
        }
        value = generator.next();
      } while (value != null);
      model.addRow(rowModel);
      return Response.ok(model).build();
    } catch (IOException e) {
      throw new WebApplicationException(e,
                  Response.Status.SERVICE_UNAVAILABLE);
    }
  }

  @GET
  @Produces(MIMETYPE_BINARY)
  public Response getBinary(final @Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("GET " + uriInfo.getAbsolutePath() + " as "+ MIMETYPE_BINARY);
    }
    servlet.getMetrics().incrementRequests(1);
    // doesn't make sense to use a non specific coordinate as this can only
    // return a single cell
    if (!rowspec.hasColumns() || rowspec.getColumns().length > 1) {
      throw new WebApplicationException(Response.Status.BAD_REQUEST);
    }
    try {
      ResultGenerator generator =
        ResultGenerator.fromRowSpec(tableResource.getName(), rowspec, null);
      if (!generator.hasNext()) {
        throw new WebApplicationException(Response.Status.NOT_FOUND);
      }
      KeyValue value = generator.next();
      byte[] family = value.getFamily();
      byte[] qualifier = value.getQualifier();
      byte[] data = tableResource.transform(family, qualifier,
        value.getValue(), Transform.Direction.OUT);
      ResponseBuilder response = Response.ok(data);
      response.header("X-Timestamp", value.getTimestamp());
      return response.build();
    } catch (IOException e) {
      throw new WebApplicationException(e,
                  Response.Status.SERVICE_UNAVAILABLE);
    }
  }

  Response update(final CellSetModel model, final boolean replace) {
    servlet.getMetrics().incrementRequests(1);
    if (servlet.isReadOnly()) {
      throw new WebApplicationException(Response.Status.FORBIDDEN);
    }
    HTablePool pool = servlet.getTablePool();
    HTableInterface table = null;
    try {
      List<RowModel> rows = model.getRows();
      List<Put> puts = new ArrayList<Put>();
      for (RowModel row: rows) {
        byte[] key = row.getKey();
        if (key == null) {
          key = rowspec.getRow();
        }
        if (key == null) {
          throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }
        Put put = new Put(key);
        int i = 0;
        for (CellModel cell: row.getCells()) {
          byte[] col = cell.getColumn();
          if (col == null) try {
            col = rowspec.getColumns()[i++];
          } catch (ArrayIndexOutOfBoundsException e) {
            col = null;
          }
          if (col == null) {
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
          }
          byte [][] parts = KeyValue.parseColumn(col);
          if (parts.length == 2 && parts[1].length > 0) {
            put.add(parts[0], parts[1], cell.getTimestamp(),
              tableResource.transform(parts[0], parts[1], cell.getValue(),
                Transform.Direction.IN));
          } else {
            put.add(parts[0], null, cell.getTimestamp(),
              tableResource.transform(parts[0], null, cell.getValue(),
                Transform.Direction.IN));
          }
        }
        puts.add(put);
        if (LOG.isDebugEnabled()) {
          LOG.debug("PUT " + put.toString());
        }
      }
      table = pool.getTable(tableResource.getName());
      table.put(puts);
      table.flushCommits();
      ResponseBuilder response = Response.ok();
      return response.build();
    } catch (IOException e) {
      throw new WebApplicationException(e,
                  Response.Status.SERVICE_UNAVAILABLE);
    } finally {
      if (table != null) {
        try {
          pool.putTable(table);
        } catch (IOException ioe) {
          throw new WebApplicationException(ioe,
              Response.Status.SERVICE_UNAVAILABLE);
        }
      }
    }
  }

  // This currently supports only update of one row at a time.
  Response updateBinary(final byte[] message, final HttpHeaders headers,
      final boolean replace) {
    servlet.getMetrics().incrementRequests(1);
    if (servlet.isReadOnly()) {
      throw new WebApplicationException(Response.Status.FORBIDDEN);
    }
    HTablePool pool = servlet.getTablePool();
    HTableInterface table = null;
    try {
      byte[] row = rowspec.getRow();
      byte[][] columns = rowspec.getColumns();
      byte[] column = null;
      if (columns != null) {
        column = columns[0];
      }
      long timestamp = HConstants.LATEST_TIMESTAMP;
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
      if (parts.length == 2 && parts[1].length > 0) {
        put.add(parts[0], parts[1], timestamp,
          tableResource.transform(parts[0], parts[1], message,
            Transform.Direction.IN));
      } else {
        put.add(parts[0], null, timestamp,
          tableResource.transform(parts[0], null, message,
            Transform.Direction.IN));
      }
      table = pool.getTable(tableResource.getName());
      table.put(put);
      if (LOG.isDebugEnabled()) {
        LOG.debug("PUT " + put.toString());
      }
      return Response.ok().build();
    } catch (IOException e) {
      throw new WebApplicationException(e,
                  Response.Status.SERVICE_UNAVAILABLE);
    } finally {
      if (table != null) {
        try {
          pool.putTable(table);
        } catch (IOException ioe) {
          throw new WebApplicationException(ioe,
              Response.Status.SERVICE_UNAVAILABLE);
        }
      }
    }
  }

  @PUT
  @Consumes({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF})
  public Response put(final CellSetModel model,
      final @Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("PUT " + uriInfo.getAbsolutePath());
    }
    return update(model, true);
  }

  @PUT
  @Consumes(MIMETYPE_BINARY)
  public Response putBinary(final byte[] message,
      final @Context UriInfo uriInfo, final @Context HttpHeaders headers) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("PUT " + uriInfo.getAbsolutePath() + " as "+ MIMETYPE_BINARY);
    }
    return updateBinary(message, headers, true);
  }

  @POST
  @Consumes({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF})
  public Response post(final CellSetModel model,
      final @Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("POST " + uriInfo.getAbsolutePath());
    }
    return update(model, false);
  }

  @POST
  @Consumes(MIMETYPE_BINARY)
  public Response postBinary(final byte[] message,
      final @Context UriInfo uriInfo, final @Context HttpHeaders headers) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("POST " + uriInfo.getAbsolutePath() + " as "+MIMETYPE_BINARY);
    }
    return updateBinary(message, headers, false);
  }

  @DELETE
  public Response delete(final @Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("DELETE " + uriInfo.getAbsolutePath());
    }
    servlet.getMetrics().incrementRequests(1);
    if (servlet.isReadOnly()) {
      throw new WebApplicationException(Response.Status.FORBIDDEN);
    }
    Delete delete = null;
    if (rowspec.hasTimestamp())
      delete = new Delete(rowspec.getRow(), rowspec.getTimestamp(), null);
    else
      delete = new Delete(rowspec.getRow());

    for (byte[] column: rowspec.getColumns()) {
      byte[][] split = KeyValue.parseColumn(column);
      if (rowspec.hasTimestamp()) {
        if (split.length == 2 && split[1].length != 0) {
          delete.deleteColumns(split[0], split[1], rowspec.getTimestamp());
        } else {
          delete.deleteFamily(split[0], rowspec.getTimestamp());
        }
      } else {
        if (split.length == 2 && split[1].length != 0) {
          delete.deleteColumns(split[0], split[1]);
        } else {
          delete.deleteFamily(split[0]);
        }
      }
    }
    HTablePool pool = servlet.getTablePool();
    HTableInterface table = null;
    try {
      table = pool.getTable(tableResource.getName());
      table.delete(delete);
      if (LOG.isDebugEnabled()) {
        LOG.debug("DELETE " + delete.toString());
      }
    } catch (IOException e) {
      throw new WebApplicationException(e, 
                  Response.Status.SERVICE_UNAVAILABLE);
    } finally {
      if (table != null) {
        try {
          pool.putTable(table);
        } catch (IOException ioe) {
          throw new WebApplicationException(ioe,
              Response.Status.SERVICE_UNAVAILABLE);
        }
      }
    }
    return Response.ok().build();
  }
}
