/**
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

package org.apache.hadoop.yarn.server.router.webapp;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebServiceProtocol;
import org.apache.hadoop.yarn.server.webapp.WebServices;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;

/**
 * Defines the contract to be implemented by the request intercepter classes,
 * that can be used to intercept and inspect messages sent from the client to
 * the resource manager server.
 *
 * This class includes 4 methods getAppAttempts, getAppAttempt, getContainers
 * and getContainer that belong to {@link WebServices}. They are in this class
 * to make sure that RouterWebServices implements the same REST methods of
 * {@code RMWebServices}.
 */
public interface RESTRequestInterceptor
    extends RMWebServiceProtocol, Configurable {

  /**
   * This method is called for initializing the intercepter. This is guaranteed
   * to be called only once in the lifetime of this instance.
   *
   * @param user the name of the client
   */
  void init(String user);

  /**
   * This method is called to release the resources held by the intercepter.
   * This will be called when the application pipeline is being destroyed. The
   * concrete implementations should dispose the resources and forward the
   * request to the next intercepter, if any.
   */
  void shutdown();

  /**
   * Sets the next intercepter in the pipeline. The concrete implementation of
   * this interface should always pass the request to the nextInterceptor after
   * inspecting the message. The last intercepter in the chain is responsible to
   * send the messages to the resource manager service and so the last
   * intercepter will not receive this method call.
   *
   * @param nextInterceptor the RESTRequestInterceptor to set in the pipeline
   */
  void setNextInterceptor(RESTRequestInterceptor nextInterceptor);

  /**
   * Returns the next intercepter in the chain.
   *
   * @return the next intercepter in the chain
   */
  RESTRequestInterceptor getNextInterceptor();

  /**
   *
   * @see WebServices#getAppAttempt(HttpServletRequest, HttpServletResponse,
   *      String, String)
   * @param req the servlet request
   * @param res the servlet response
   * @param appId the application we want to get the appAttempt. It is a
   *          PathParam.
   * @param appAttemptId the AppAttempt we want to get the info. It is a
   *          PathParam.
   * @return AppAttemptInfo of the specific AppAttempt
   */
  AppAttemptInfo getAppAttempt(HttpServletRequest req, HttpServletResponse res,
      String appId, String appAttemptId);

  /**
   *
   * @see WebServices#getContainers(HttpServletRequest, HttpServletResponse,
   *      String, String)
   * @param req the servlet request
   * @param res the servlet response
   * @param appId the application we want to get the containers info. It is a
   *          PathParam.
   * @param appAttemptId the AppAttempt we want to get the info. It is a
   *          PathParam.
   * @return ContainersInfo of all the containers that belong to the specific
   *         AppAttempt
   */
  ContainersInfo getContainers(HttpServletRequest req, HttpServletResponse res,
      String appId, String appAttemptId);

  /**
   *
   * @see WebServices#getContainer(HttpServletRequest, HttpServletResponse,
   *      String, String, String)
   * @param req the servlet request
   * @param res the servlet response
   * @param appId the application we want to get the containers info. It is a
   *          PathParam.
   * @param appAttemptId the AppAttempt we want to get the info. It is a
   *          PathParam.
   * @param containerId the container we want to get the info. It is a
   *          PathParam.
   * @return ContainerInfo of the specific ContainerId
   */
  ContainerInfo getContainer(HttpServletRequest req, HttpServletResponse res,
      String appId, String appAttemptId, String containerId);
}