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

package org.apache.hadoop.yarn.api;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.io.retry.AtMostOnce;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceBlacklistRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * <p>The protocol between a live instance of <code>ApplicationMaster</code> 
 * and the <code>ResourceManager</code>.</p>
 * 
 * <p>This is used by the <code>ApplicationMaster</code> to register/unregister
 * and to request and obtain resources in the cluster from the
 * <code>ResourceManager</code>.</p>
 */
@Public
@Stable
public interface ApplicationMasterProtocol {

  /**
   * <p>
   * The interface used by a new <code>ApplicationMaster</code> to register with
   * the <code>ResourceManager</code>.
   * </p>
   * 
   * <p>
   * The <code>ApplicationMaster</code> needs to provide details such as RPC
   * Port, HTTP tracking url etc. as specified in
   * {@link RegisterApplicationMasterRequest}.
   * </p>
   * 
   * <p>
   * The <code>ResourceManager</code> responds with critical details such as
   * maximum resource capabilities in the cluster as specified in
   * {@link RegisterApplicationMasterResponse}.
   * </p>
   * 
   * @param request
   *          registration request
   * @return registration respose
   * @throws YarnException
   * @throws IOException
   * @throws InvalidApplicationMasterRequestException
   *           The exception is thrown when an ApplicationMaster tries to
   *           register more then once.
   * @see RegisterApplicationMasterRequest
   * @see RegisterApplicationMasterResponse
   */
  @Public
  @Stable
  @Idempotent
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request) 
  throws YarnException, IOException;
  
  /**
   * <p>The interface used by an <code>ApplicationMaster</code> to notify the 
   * <code>ResourceManager</code> about its completion (success or failed).</p>
   * 
   * <p>The <code>ApplicationMaster</code> has to provide details such as 
   * final state, diagnostics (in case of failures) etc. as specified in 
   * {@link FinishApplicationMasterRequest}.</p>
   * 
   * <p>The <code>ResourceManager</code> responds with 
   * {@link FinishApplicationMasterResponse}.</p>
   * 
   * @param request completion request
   * @return completion response
   * @throws YarnException
   * @throws IOException
   * @see FinishApplicationMasterRequest
   * @see FinishApplicationMasterResponse
   */
  @Public
  @Stable
  @AtMostOnce
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request) 
  throws YarnException, IOException;

  /**
   * <p>
   * The main interface between an <code>ApplicationMaster</code> and the
   * <code>ResourceManager</code>.
   * </p>
   * 
   * <p>
   * The <code>ApplicationMaster</code> uses this interface to provide a list of
   * {@link ResourceRequest} and returns unused {@link Container} allocated to
   * it via {@link AllocateRequest}. Optionally, the
   * <code>ApplicationMaster</code> can also <em>blacklist</em> resources which
   * it doesn't want to use.
   * </p>
   * 
   * <p>
   * This also doubles up as a <em>heartbeat</em> to let the
   * <code>ResourceManager</code> know that the <code>ApplicationMaster</code>
   * is alive. Thus, applications should periodically make this call to be kept
   * alive. The frequency depends on
   * {@link YarnConfiguration#RM_AM_EXPIRY_INTERVAL_MS} which defaults to
   * {@link YarnConfiguration#DEFAULT_RM_AM_EXPIRY_INTERVAL_MS}.
   * </p>
   * 
   * <p>
   * The <code>ResourceManager</code> responds with list of allocated
   * {@link Container}, status of completed containers and headroom information
   * for the application.
   * </p>
   * 
   * <p>
   * The <code>ApplicationMaster</code> can use the available headroom
   * (resources) to decide how to utilized allocated resources and make informed
   * decisions about future resource requests.
   * </p>
   * 
   * @param request
   *          allocation request
   * @return allocation response
   * @throws YarnException
   * @throws IOException
   * @throws InvalidApplicationMasterRequestException
   *           This exception is thrown when an ApplicationMaster calls allocate
   *           without registering first.
   * @throws InvalidResourceBlacklistRequestException
   *           This exception is thrown when an application provides an invalid
   *           specification for blacklist of resources.
   * @throws InvalidResourceRequestException
   *           This exception is thrown when a {@link ResourceRequest} is out of
   *           the range of the configured lower and upper limits on the
   *           resources.
   * @see AllocateRequest
   * @see AllocateResponse
   */
  @Public
  @Stable
  @AtMostOnce
  public AllocateResponse allocate(AllocateRequest request) 
  throws YarnException, IOException;
}
