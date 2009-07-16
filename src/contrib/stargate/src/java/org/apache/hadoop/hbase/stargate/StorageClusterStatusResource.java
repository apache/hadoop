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
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.stargate.model.StorageClusterStatusModel;

@Path(Constants.PATH_STATUS_CLUSTER)
public class StorageClusterStatusResource implements Constants {
  private static final Log LOG =
    LogFactory.getLog(StorageClusterStatusResource.class);

  private CacheControl cacheControl;

  public StorageClusterStatusResource() {
    cacheControl = new CacheControl();
    cacheControl.setNoCache(true);
    cacheControl.setNoTransform(false);
  }

  @GET
  @Produces({MIMETYPE_TEXT, MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_JAVASCRIPT,
    MIMETYPE_PROTOBUF})
  public Response get(@Context UriInfo uriInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("GET " + uriInfo.getAbsolutePath());
    }
    try {
      RESTServlet server = RESTServlet.getInstance();
      HBaseAdmin admin = new HBaseAdmin(server.getConfiguration());
      ClusterStatus status = admin.getClusterStatus();
      StorageClusterStatusModel model = new StorageClusterStatusModel();
      model.setRegions(status.getRegionsCount());
      model.setRequests(status.getRequestsCount());
      model.setAverageLoad(status.getAverageLoad());
      for (HServerInfo info: status.getServerInfo()) {
        HServerLoad load = info.getLoad();
        StorageClusterStatusModel.Node node = 
          model.addLiveNode(
            info.getServerAddress().getHostname() + ":" + 
            Integer.toString(info.getServerAddress().getPort()),
            info.getStartCode(), load.getUsedHeapMB(),
            load.getMaxHeapMB());
        node.setRequests(load.getNumberOfRequests());
        for (HServerLoad.RegionLoad region: load.getRegionsLoad()) {
          node.addRegion(region.getName(), region.getStores(),
            region.getStorefiles(), region.getStorefileSizeMB(),
            region.getMemStoreSizeMB(), region.getStorefileIndexSizeMB());
        }
      }
      for (String name: status.getDeadServerNames()) {
        model.addDeadNode(name);
      }
      ResponseBuilder response = Response.ok(model);
      response.cacheControl(cacheControl);
      return response.build();
    } catch (IOException e) {
      throw new WebApplicationException(e, 
                  Response.Status.SERVICE_UNAVAILABLE);
    }
  }
}
