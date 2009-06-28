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

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.stargate.model.CellModel;
import org.apache.hadoop.hbase.stargate.model.CellSetModel;
import org.apache.hadoop.hbase.stargate.model.RowModel;
import org.apache.hadoop.hbase.util.Bytes;

import com.sun.jersey.core.util.Base64;

public class ScannerInstanceResource implements Constants {
  private static final Log LOG =
    LogFactory.getLog(ScannerInstanceResource.class);

  protected ResultGenerator generator;
  private String id;
  private int batch;
  private CacheControl cacheControl;

  public ScannerInstanceResource(String table, String id, 
      ResultGenerator generator, int batch) throws IOException {
    this.id = id;
    this.generator = generator;
    this.batch = batch;
    cacheControl = new CacheControl();
    cacheControl.setNoCache(true);
    cacheControl.setNoTransform(false);
  }

  @GET
  @Produces({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_JAVASCRIPT,
    MIMETYPE_PROTOBUF})
  public Response get(@Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("GET " + uriInfo.getAbsolutePath());
    }
    CellSetModel model = new CellSetModel();
    RowModel rowModel = null;
    byte[] rowKey = null;
    int count = batch;
    do {
      KeyValue value = null;
      try {
        value = generator.next();
      } catch (IllegalStateException e) {
        ScannerResource.delete(id);
        throw new WebApplicationException(Response.Status.GONE);
      }
      if (value == null) {
        LOG.info("generator exhausted");
        // respond with 204 (No Content) if an empty cell set would be
        // returned
        if (count == batch) {
          return Response.noContent().build();
        }
        break;
      }
      if (rowKey == null) {
        rowKey = value.getRow();
        rowModel = new RowModel(rowKey);
      }
      if (!Bytes.equals(value.getRow(), rowKey)) {
        model.addRow(rowModel);
        rowKey = value.getRow();
        rowModel = new RowModel(rowKey);
      }
      rowModel.addCell(
        new CellModel(value.getColumn(), value.getTimestamp(),
              value.getValue()));
    } while (--count > 0);
    model.addRow(rowModel);
    ResponseBuilder response = Response.ok(model);
    response.cacheControl(cacheControl);
    return response.build();
  }

  @GET
  @Produces(MIMETYPE_BINARY)
  public Response getBinary(@Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("GET " + uriInfo.getAbsolutePath() + " as " +
        MIMETYPE_BINARY);
    }
    try {
      KeyValue value = generator.next();
      if (value == null) {
        LOG.info("generator exhausted");
        return Response.noContent().build();
      }
      ResponseBuilder response = Response.ok(value.getValue());
      response.cacheControl(cacheControl);
      response.header("X-Row", Base64.encode(value.getRow()));
      response.header("X-Column", Base64.encode(value.getColumn()));
      response.header("X-Timestamp", value.getTimestamp());
      return response.build();
    } catch (IllegalStateException e) {
      ScannerResource.delete(id);
      throw new WebApplicationException(Response.Status.GONE);
    }
  }

  @DELETE
  public Response delete(@Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("DELETE " + uriInfo.getAbsolutePath());
    }
    ScannerResource.delete(id);
    return Response.ok().build();
  }
}
