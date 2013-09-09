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
package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import junit.framework.Assert;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRegistrationEvent;

import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.junit.Test;
import org.mockito.Mockito;

public class TestRMAppAttemptImpl {
  
  private void testTrackingUrl(String url, boolean unmanaged) {
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance
        (ApplicationId.newInstance(1, 2), 1);
    EventHandler handler = Mockito.mock(EventHandler.class);    
    Dispatcher dispatcher = Mockito.mock(Dispatcher.class);
    Mockito.when(dispatcher.getEventHandler()).thenReturn(handler);
    RMContext rmContext = Mockito.mock(RMContext.class);
    Mockito.when(rmContext.getDispatcher()).thenReturn(dispatcher);
    
    ApplicationSubmissionContext appContext = 
        Mockito.mock(ApplicationSubmissionContext.class);
    Mockito.when(appContext.getUnmanagedAM()).thenReturn(unmanaged);
    
    RMAppAttemptImpl attempt = new RMAppAttemptImpl(attemptId, rmContext, null, 
        null, appContext, new YarnConfiguration(), null);
    RMAppAttemptRegistrationEvent event = 
        Mockito.mock(RMAppAttemptRegistrationEvent.class);
    Mockito.when(event.getHost()).thenReturn("h");
    Mockito.when(event.getRpcport()).thenReturn(0);
    Mockito.when(event.getTrackingurl()).thenReturn(url);
    new RMAppAttemptImpl.AMRegisteredTransition().transition(attempt, event);
    if (unmanaged) {
      Assert.assertEquals(url, attempt.getTrackingUrl());
    } else {
      Assert.assertNotSame(url, attempt.getTrackingUrl());
      Assert.assertTrue(attempt.getTrackingUrl().contains(
          ProxyUriUtils.PROXY_SERVLET_NAME));
      Assert.assertTrue(attempt.getTrackingUrl().contains(
          attemptId.getApplicationId().toString()));
    }
  }

  @Test
  public void testTrackingUrlUnmanagedAM() {
    testTrackingUrl("http://foo:8000/x", true);
  }

  @Test
  public void testTrackingUrlManagedAM() {
    testTrackingUrl("bar:8000/x", false);
  }
}
