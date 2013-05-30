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
package org.apache.hadoop.mapreduce.v2.app.webapp;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.webapp.Controller.RequestContext;
import org.junit.Before;
import org.junit.Test;

public class TestAppController {

  private AppController appController;
  private RequestContext ctx;

  @Before
  public void setUp() {
    AppContext context = mock(AppContext.class);
    when(context.getApplicationID()).thenReturn(
        ApplicationId.newInstance(0, 0));
    App app = new App(context);
    Configuration conf = new Configuration();
    ctx = mock(RequestContext.class);
    appController = new AppController(app, conf, ctx);
  }

  @Test
  public void testBadRequest() {
    String message = "test string";
    appController.badRequest(message);
    verifyExpectations(message);
  }

  @Test
  public void testBadRequestWithNullMessage() {
    // It should not throw NullPointerException
    appController.badRequest(null);
    verifyExpectations(StringUtils.EMPTY);
  }

  private void verifyExpectations(String message) {
    verify(ctx).setStatus(400);
    verify(ctx).set("app.id", "application_0_0000");
    verify(ctx).set(eq("rm.web"), anyString());
    verify(ctx).set("title", "Bad request: " + message);
  }
}
