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

package org.apache.slider.server.services.workflow;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestWorkflowCompositeService extends ParentWorkflowTestBase {
  private static final Logger
      log = LoggerFactory.getLogger(TestWorkflowCompositeService.class);

  @Test
  public void testSingleChild() throws Throwable {
    Service parent = startService(new MockService());
    parent.stop();
  }

  @Test
  public void testSingleChildTerminating() throws Throwable {
    ServiceParent parent =
        startService(new MockService("1", false, 100));
    waitForParentToStop(parent);
  }

  @Test
  public void testSingleChildFailing() throws Throwable {
    ServiceParent parent =
        startService(new MockService("1", true, 100));
    waitForParentToStop(parent);
    assert parent.getFailureCause() != null;
  }

  @Test
  public void testTwoChildren() throws Throwable {
    MockService one = new MockService("one", false, 100);
    MockService two = new MockService("two", false, 100);
    ServiceParent parent = startService(one, two);
    waitForParentToStop(parent);
    assertStopped(one);
    assertStopped(two);
  }

  @Test
  public void testCallableChild() throws Throwable {

    MockService one = new MockService("one", false, 100);
    CallableHandler handler = new CallableHandler("hello");
    WorkflowCallbackService<String> ens =
        new WorkflowCallbackService<String>("handler", handler, 100, true);
    MockService two = new MockService("two", false, 100);
    ServiceParent parent = startService(one, ens, two);
    waitForParentToStop(parent);
    assertStopped(one);
    assertStopped(ens);
    assertStopped(two);
    assertTrue(handler.notified);
    String s = ens.getScheduledFuture().get();
    assertEquals("hello", s);
  }

  @Test
  public void testNestedComposite() throws Throwable {
    MockService one = new MockService("one", false, 100);
    MockService two = new MockService("two", false, 100);
    ServiceParent parent = buildService(one, two);
    ServiceParent outer = startService(parent);
    assertTrue(outer.waitForServiceToStop(1000));
    assertStopped(one);
    assertStopped(two);
  }

  @Test
  public void testFailingComposite() throws Throwable {
    MockService one = new MockService("one", true, 10);
    MockService two = new MockService("two", false, 1000);
    ServiceParent parent = startService(one, two);
    waitForParentToStop(parent);
    assertStopped(one);
    assertStopped(two);
    assertNotNull(one.getFailureCause());
    assertNotNull(parent.getFailureCause());
    assertEquals(one.getFailureCause(), parent.getFailureCause());
  }

  @Override
  public ServiceParent buildService(Service... services) {
    ServiceParent parent =
        new WorkflowCompositeService("test", services);
    parent.init(new Configuration());
    return parent;
  }

}
