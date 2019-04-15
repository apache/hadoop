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

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.hadoop.yarn.appcatalog.application.AppCatalogSolrClient;
import org.apache.hadoop.yarn.appcatalog.application.YarnServiceClient;
import org.apache.hadoop.yarn.appcatalog.model.AppEntry;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.solr.client.solrj.SolrServerException;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * Application catalog REST API for displaying application details.
 */
@Path("/app_details")
public class AppDetailsController {

  public AppDetailsController() {
  }

  /**
   * List detail information about the deployed application.
   *
   * @apiGroup AppDetailController
   * @apiName getDetails
   * @api {get} /app_details/config/{id}  Check config of application instance.
   * @apiParam {String} id Application ID to fetch configuration.
   * @apiSuccess {Object} AppEntry Application configuration.
   * @apiSuccessExample {json} Success-Response:
   *     HTTP/1.1 200 OK
   *     {
   *       "id":"howita_man",
   *        "name":"howita_man",
   *        "app":"Jenkins-ci.org/Jenkins",
   *        "yarnfile":{
   *           "name":"howita_man",
   *           "lifetime":3600,
   *           "containers":[
   *           ],
   *           "components":[
   *              {
   *                 "name":"jenkins",
   *                 "dependencies":[
   *                 ],
   *                 "artifact":{
   *                    "id":"eyang-1.openstacklocal:5000/jenkins:latest",
   *                    "type":"DOCKER"
   *                 },
   *                 "launch_command":"",
   *                 "resource":{
   *                    "uri":null,
   *                    "profile":null,
   *                    "cpus":1,
   *                    "memory":"2048"
   *                 },
   *                 "number_of_containers":1,
   *                 "run_privileged_container":false,
   *                 "configuration":{
   *                    "properties":{
   *                    },
   *                    "env":{
   *                    },
   *                    "files":[
   *                    ]
   *                 },
   *                 "quicklinks":[
   *                 ],
   *                 "containers":[
   *                 ]
   *              }
   *           ],
   *           "configuration":{
   *              "properties":{
   *              },
   *              "env":{
   *              },
   *              "files":[
   *              ]
   *           },
   *           "quicklinks":{
   *              "Jenkins UI":"http://jenkins.howita_man.yarn.${DOMAIN}:8080/"
   *           }
   *        }
   *     }
   * @param id - Application ID
   * @return application entry-
   */
  @Path("config/{id}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public AppEntry getDetails(@PathParam("id") String id) {
    AppCatalogSolrClient sc = new AppCatalogSolrClient();
    return sc.findAppEntry(id);
  }

  /**
   * Check application status.
   *
   * @apiGroup AppDetailController
   * @apiName getStatus
   * @api {get} /app_details/status/{id}  Check status of application instance.
   * @apiParam {String} id Application ID to check.
   * @apiSuccessExample {json} Success-Response:
   *     HTTP/1.1 200 OK
   *     {
   *       "id":"howita_man",
   *        "name":"howita_man",
   *        "app":"Jenkins-ci.org/Jenkins",
   *        "yarnfile":{
   *           "name":"howita_man",
   *           "lifetime":3099,
   *           "containers":[
   *           ],
   *           "id":"application_1503694240849_0043",
   *           "components":[
   *              {
   *                 "name":"jenkins",
   *                 "dependencies":[
   *                 ],
   *                 "artifact":{
   *                    "id":"eyang-1.openstacklocal:5000/jenkins:latest",
   *                    "type":"DOCKER"
   *                 },
   *                 "launch_command":"",
   *                 "resource":{
   *                    "uri":null,
   *                    "profile":null,
   *                    "cpus":1,
   *                    "memory":"2048"
   *                 },
   *                 "number_of_containers":1,
   *                 "run_privileged_container":false,
   *                 "configuration":{
   *                    "properties":{
   *                    },
   *                    "env":{
   *                    },
   *                    "files":[
   *                    ]
   *                 },
   *                 "quicklinks":[
   *                 ],
   *                 "containers":[
   *                    {
   *                       "id":"container_1503694240849_0043_01_000002",
   *                       "launch_time":1504630535403,
   *                       "bare_host":"eyang-4.openstacklocal",
   *                       "state":"READY",
   *                       "component_name":"jenkins-0"
   *                    }
   *                 ]
   *              }
   *           ],
   *           "configuration":{
   *              "properties":{
   *              },
   *              "env":{
   *              },
   *              "files":[
   *              ]
   *           },
   *           "quicklinks":{
   *              "Jenkins UI":"http://jenkins.howita_man.yarn.${DOMAIN}:8080/"
   *           }
   *        }
   *     }
   * @apiSuccess {Object} text Give status
   * @param id - Application ID
   * @return application entry
   */
  @Path("status/{id}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public AppEntry getStatus(@PathParam("id") String id) {
    AppCatalogSolrClient sc = new AppCatalogSolrClient();
    AppEntry appEntry = sc.findAppEntry(id);
    YarnServiceClient yc = new YarnServiceClient();
    yc.getStatus(appEntry);
    return appEntry;
  }

  /**
   * Stop an application.
   *
   * @apiGroup AppDetailController
   * @apiName stopApp
   * @api {post} /app_details/stop/{id}  Stop one instance of application.
   * @apiParam {String} id Application ID to stop.
   * @apiSuccess {String} text Give deployment status
   * @apiError BadRequest Requested application does not stop.
   * @param id - Application ID
   * @return Web response code
   */
  @Path("stop/{id}")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public Response stopApp(@PathParam("id") String id) {
    AppCatalogSolrClient sc = new AppCatalogSolrClient();
    AppEntry app = sc.findAppEntry(id);
    Service yarnApp = app.getYarnfile();
    yarnApp.setState(ServiceState.STOPPED);
    try {
      YarnServiceClient yc = new YarnServiceClient();
      yc.stopApp(yarnApp);
    } catch (JsonProcessingException e) {
      return Response.status(Status.BAD_REQUEST).build();
    }
    return Response.ok().build();
  }

  /**
   * Restart an application.
   *
   * @apiGroup AppDetailController
   * @apiName restartApp
   * @api {post} /app_details/restart/{id}  Restart one instance of application.
   * @apiParam {String} id Application ID to restart.
   * @apiSuccess {String} text Give deployment status
   * @apiError BadRequest Requested application does not restart.
   * @param id - Application ID
   * @return Web response code
   */
  @Path("restart/{id}")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public Response restartApp(@PathParam("id") String id) {
    AppCatalogSolrClient sc = new AppCatalogSolrClient();
    AppEntry app = sc.findAppEntry(id);
    Service yarnApp = app.getYarnfile();
    yarnApp.setState(ServiceState.STARTED);
    try {
      YarnServiceClient yc = new YarnServiceClient();
      yc.restartApp(yarnApp);
    } catch (JsonProcessingException e) {
      return Response.status(Status.BAD_REQUEST).build();
    }
    return Response.ok().build();
  }

  /**
   * Upgrade an application.
   *
   * @apiGroup AppDetailController
   * @apiName upgradeApp
   * @api {put} /app_details/upgrade/{id} Upgrade one instance of application.
   * @apiParam {String} id Application Name to upgrade.
   * @apiSuccess {String} text
   * @apiError BadRequest Requested application does not upgrade.
   * @return Web response code
   */
  @Path("upgrade/{id}")
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response upgradeApp(@PathParam("id") String id, Service app) {
    try {
      AppCatalogSolrClient sc = new AppCatalogSolrClient();
      sc.upgradeApp(app);
      YarnServiceClient yc = new YarnServiceClient();
      yc.upgradeApp(app);
    } catch (IOException | SolrServerException e) {
      return Response.status(Status.BAD_REQUEST).entity(e.toString()).build();
    }
    String output = "{\"status\":\"Application upgrade requested.\",\"id\":\"" +
        app.getName() + "\"}";
    return Response.status(Status.ACCEPTED).entity(output).build();
  }
}
