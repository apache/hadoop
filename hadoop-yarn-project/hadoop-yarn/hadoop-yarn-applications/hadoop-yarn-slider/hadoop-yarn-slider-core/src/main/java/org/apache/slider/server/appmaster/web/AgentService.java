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
package org.apache.slider.server.appmaster.web;

import org.apache.slider.server.appmaster.web.rest.agent.AgentWebApp;
import org.apache.slider.server.services.workflow.ClosingService;
import org.apache.slider.server.services.workflow.WorkflowCompositeService;

/**
 * agent service gives the agent webapp lifecycle integration
 */
public class AgentService extends ClosingService<AgentWebApp> {


  public AgentService(String name) {
    super(name);
  }

  public AgentService(String name, AgentWebApp app) {
    super(name, app);
  }

}
