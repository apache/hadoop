/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.server.appmaster.web.rest.agent;

import org.apache.slider.server.appmaster.web.WebAppApi;
import org.apache.slider.server.appmaster.web.rest.RestPaths;

import javax.ws.rs.Path;
import javax.ws.rs.core.Context;

/** The available agent REST services exposed by a slider AM. */
@Path(RestPaths.SLIDER_AGENT_CONTEXT_ROOT)
public class AgentWebServices {
  /** AM/WebApp info object */
  @Context
  private WebAppApi slider;

  public AgentWebServices() {
  }

  @Path(RestPaths.SLIDER_SUBPATH_AGENTS)
  public AgentResource getAgentResource () {
    return new AgentResource(slider);
  }

}
