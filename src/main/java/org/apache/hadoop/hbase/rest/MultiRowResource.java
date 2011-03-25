/**
 * Copyright 2011 The Apache Software Foundation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.rest.ResourceBase;
import org.apache.hadoop.hbase.rest.RowSpec;
import org.apache.hadoop.hbase.rest.TableResource;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;
import org.apache.hadoop.hbase.rest.transform.Transform;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.ArrayList;

public class MultiRowResource extends ResourceBase {
  private static final Log LOG = LogFactory.getLog(MultiRowResource.class);
  public static final String ROW_KEYS_PARAM_NAME = "row";

  TableResource tableResource;
  Integer versions = null;

  /**
   * Constructor
   *
   * @param tableResource
   * @param versions
   * @throws java.io.IOException
   */
  public MultiRowResource(TableResource tableResource, String versions) throws IOException {
    super();
    this.tableResource = tableResource;

    if (versions != null) {
      this.versions = Integer.valueOf(versions);

    }
  }

  @GET
  @Produces({MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF})
  public Response get(final @Context UriInfo uriInfo) {
    MultivaluedMap<String, String> params = uriInfo.getQueryParameters();

    servlet.getMetrics().incrementRequests(1);
    try {
      CellSetModel model = new CellSetModel();
      for (String rk : params.get(ROW_KEYS_PARAM_NAME)) {
        RowSpec rowSpec = new RowSpec(rk);

        if (this.versions != null) {
          rowSpec.setMaxVersions(this.versions);
        }

        ResultGenerator generator = ResultGenerator.fromRowSpec(this.tableResource.getName(), rowSpec, null);
        if (!generator.hasNext()) {
          throw new WebApplicationException(Response.Status.NOT_FOUND);
        }

        KeyValue value = null;
        RowModel rowModel = new RowModel(rk);


        while ((value = generator.next()) != null) {
          byte[] family = value.getFamily();
          byte[] qualifier = value.getQualifier();
          byte[] data = tableResource.transform(family, qualifier, value.getValue(), Transform.Direction.OUT);
          rowModel.addCell(new CellModel(family, qualifier, value.getTimestamp(), data));
        }

        model.addRow(rowModel);
      }
      return Response.ok(model).build();
    } catch (IOException e) {
      throw new WebApplicationException(e,
              Response.Status.SERVICE_UNAVAILABLE);
    }

  }
}
