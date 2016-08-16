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
package org.apache.slider.server.appmaster.web.rest.publisher;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.providers.agent.AgentProviderService;
import org.apache.slider.server.appmaster.actions.QueueAccess;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class TestAgentProviderService extends AgentProviderService {
  protected static final Logger log =
      LoggerFactory.getLogger(TestAgentProviderService.class);

  public TestAgentProviderService() {
    super();
    log.info("TestAgentProviderService created");
  }

  @Override
  public void bind(StateAccessForProviders stateAccessor,
      QueueAccess queueAccess,
      List<Container> liveContainers) {
    super.bind(stateAccessor, queueAccess, liveContainers);
    Map<String,String> dummyProps = new HashMap<String, String>();
    dummyProps.put("prop1", "val1");
    dummyProps.put("prop2", "val2");
    log.info("publishing dummy-site.xml with values {}", dummyProps);
    publishApplicationInstanceData("dummy-site", "dummy configuration",
                                   dummyProps.entrySet());
    // publishing global config for testing purposes
    publishApplicationInstanceData("global", "global configuration",
                                   stateAccessor.getAppConfSnapshot()
                                       .getGlobalOptions().entrySet());
  }

}
