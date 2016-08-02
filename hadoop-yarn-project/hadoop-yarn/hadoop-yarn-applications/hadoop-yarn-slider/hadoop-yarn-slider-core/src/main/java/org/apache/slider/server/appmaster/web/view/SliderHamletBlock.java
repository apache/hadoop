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

package org.apache.slider.server.appmaster.web.view;

import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.slider.providers.ProviderService;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.apache.slider.server.appmaster.web.rest.RestPaths;

import static org.apache.hadoop.yarn.util.StringHelper.ujoin;
import static org.apache.slider.server.appmaster.web.rest.RestPaths.SLIDER_PATH_APPLICATION;

/**
 * Anything we want to share across slider hamlet blocks
 */
public abstract class SliderHamletBlock extends HtmlBlock  {

  protected final StateAccessForProviders appState;
  protected final ProviderService providerService;
  protected final RestPaths restPaths = new RestPaths();
  
  public SliderHamletBlock(WebAppApi slider) {
    this.appState = slider.getAppState();
    this.providerService = slider.getProviderService();
  }

  protected String rootPath(String absolutePath) {
    return root_url(absolutePath);
  }

  protected String relPath(String... args) {
    return ujoin(this.prefix(), args);
  }

  protected String apiPath(String api) {
    return root_url(SLIDER_PATH_APPLICATION,  api);
  }

}
