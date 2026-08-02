/*
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

package org.apache.hadoop.yarn.appcatalog.controller;

import java.io.IOException;
import java.util.List;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

import org.apache.hadoop.yarn.appcatalog.application.AppCatalogSolrClient;
import org.apache.hadoop.yarn.appcatalog.application.YarnServiceClient;
import org.apache.hadoop.yarn.appcatalog.model.AppEntry;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.solr.client.solrj.SolrServerException;

/**
 * Application deployment module.
 */
@Path("/app_list")
@Produces({ MediaType.APPLICATION_JSON })
public class AppListController {

  public AppListController() {
  }

  /**
   * Get Application List.
   *
   * <pre>{@code
   * @apiGroup AppListController
   * @apiName get
   * @api {get} /app_list  Get list of deployed applications.
   * @apiSuccess {Object[]}  List<AppEntry> List of deployed Applications.
   * @apiSuccessExample {json} Success-Response:
   *     HTTP/1.1 200 OK
   *     [
   *        {
   *           "id":"howita-man",
   *           "name":"howita-man",
   *           "app":"Jenkins-ci.org/Jenkins",
   *           "yarnfile":{
   *              "name":"howita_man",
   *              "lifetime":3600,
   *              "containers":[
   *              ],
   *              "components":[
   *                 {
   *                    "name":"jenkins",
   *                    "dependencies":[
   *                    ],
   *                    "artifact":{
   *                       "id":"eyang-1.openstacklocal:5000/jenkins:latest",
   *                       "type":"DOCKER"
   *                    },
   *                    "launch_command":"",
   *                    "resource":{
   *                       "uri":null,
   *                       "profile":null,
   *                       "cpus":1,
   *                       "memory":"2048"
   *                    },
   *                    "number_of_containers":1,
   *                    "run_privileged_container":false,
   *                    "configuration":{
   *                       "properties":{
   *                       },
   *                       "env":{
   *                       },
   *                       "files":[
   *                       ]
   *                    },
   *                    "quicklinks":[
   *                    ],
   *                    "containers":[
   *                    ]
   *                 }
   *              ],
   *              "configuration":{
   *                 "properties":{
   *                 },
   *                 "env":{
   *                 },
   *                 "files":[
   *                 ]
   *              },
   *              "quicklinks":{
   *                 "Jenkins UI":"http://jenkins.${SERVICE_NAME}.${USER}.${DOMAIN}:8080/"
   *              }
   *           }
   *        },
   *        {
   *        ...
   *        }
   *     ]
   * }</pre>
   *
   * @return Active application deployed by current user.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public List<AppEntry> getList() {
    AppCatalogSolrClient sc = new AppCatalogSolrClient();
    return sc.listAppEntries();
  }

  /**
   * Delete an application.
   *
   * <pre>{@code
   * @apiGroup AppListController
   * @apiName delete
   * @api {delete} /app_list  Delete one instance of application.
   * @apiParam {String} id Application name to delete.
   * @apiSuccess {String} text Delete request accepted
   * }</pre>
   *
   * @param id application ID
   * @param name application name
   * @return Web response
   */
  @DELETE
  @Path("{id}/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response delete(@PathParam("id") String id,
      @PathParam("name") String name) {
    AppCatalogSolrClient sc = new AppCatalogSolrClient();
    sc.deleteApp(id);
    YarnServiceClient yc = new YarnServiceClient();
    yc.deleteApp(name);
    return Response.status(Status.ACCEPTED).build();
  }

  /**
   * Deploy an application.
   *
   * <pre>{@code
   * @apiGroup AppListController
   * @apiName deploy
   * @api {post} /app_list/{id}  Deploy one instance of application.
   * @apiParam {String} id Application ID to deploy.
   * @apiSuccess {String} text Give deployment status
   * @apiError BadRequest Unable to deploy requested application.
   * }</pre>
   *
   * @param id application ID
   * @return Web response
   */
  @POST
  @Path("{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response deploy(@PathParam("id") String id, Service service) {
    AppCatalogSolrClient sc = new AppCatalogSolrClient();
    try {
      sc.deployApp(id, service);
    } catch (SolrServerException | IOException e) {
      return Response.status(Status.BAD_REQUEST).entity(e.toString()).build();
    }
    YarnServiceClient yc = new YarnServiceClient();
    yc.createApp(service);
    String output = "{\"status\":\"Application deployed.\",\"id\":\"" +
        service.getName() + "\"}";
    return Response.status(Status.ACCEPTED).entity(output).build();
  }

}
