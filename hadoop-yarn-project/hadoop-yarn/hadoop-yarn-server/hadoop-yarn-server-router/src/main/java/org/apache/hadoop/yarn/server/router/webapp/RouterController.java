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

import org.apache.hadoop.yarn.webapp.Controller;

import com.google.inject.Inject;

/**
 * Controller for the Router Web UI.
 */
public class RouterController extends Controller {

  @Inject
  RouterController(RequestContext ctx) {
    super(ctx);
  }

  @Override
  public void index() {
    setTitle("Router");
    render(AboutPage.class);
  }

  public void about() {
    setTitle("About the Cluster");
    render(AboutPage.class);
  }

  public void federation() {
    setTitle("Federation");
    render(FederationPage.class);
  }

  public void apps() {
    setTitle("Applications");
    render(AppsPage.class);
  }

  public void nodes() {
    setTitle("Nodes");
    render(NodesPage.class);
  }
}
