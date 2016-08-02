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

package org.apache.slider.server.services.utility;

import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.webapp.WebApp;

/**
 * Contains a webapp reference and stops it in teardown if non-null
 * <p>
 * It does not start the application.
 * Access to the field is not synchronized across threads; it is the
 * responsibility of the caller.
 */
public class WebAppService<T extends WebApp> extends AbstractService {

  private volatile T webApp;

  public WebAppService(String name) {
    super(name);
  }

  public WebAppService(String name, T app) {
    super(name);
    webApp = app;
  }

  public T getWebApp() {
    return webApp;
  }

  public void setWebApp(T webApp) {
    this.webApp = webApp;
  }


  @Override
  protected void serviceStart() throws Exception {

  }

  /**
   * Stop operation stops the webapp; sets the reference to null
   * @throws Exception
   */
  @Override
  protected void serviceStop() throws Exception {
    if (webApp != null) {
      webApp.stop();
      webApp = null;
    }
  }
}
