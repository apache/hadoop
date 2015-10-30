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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.join;

import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

import com.google.inject.Inject;

public class NMController extends Controller implements YarnWebParams {
  
  @Inject
  public NMController(RequestContext requestContext) {
    super(requestContext);
  }

  @Override
  // TODO: What use of this with info() in?
  public void index() {
    setTitle(join("NodeManager - ", $(NM_NODENAME)));
  }

  public void info() {
    render(NodePage.class);    
  }

  public void node() {
    render(NodePage.class);
  }

  public void allApplications() {
    render(AllApplicationsPage.class);
  }

  public void allContainers() {
    render(AllContainersPage.class);
  }

  public void application() {
    render(ApplicationPage.class);
  }

  public void container() {
    render(ContainerPage.class);
  }

  public void errorsAndWarnings() {
    render(NMErrorsAndWarningsPage.class);
  }

  public void logs() {
    render(ContainerLogsPage.class);
  }
}
