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

package org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.placement.MockQueueHierarchyBuilder;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestMappingRuleValidationContextImpl {
  @Test
  public void testContextVariables() throws YarnException {
    //Setting up queue manager and emulated queue hierarchy
    CapacitySchedulerQueueManager qm =
        mock(CapacitySchedulerQueueManager.class);

    MockQueueHierarchyBuilder.create()
        .withQueueManager(qm)
        .withQueue("root.unmanaged")
        .build();
    when(qm.getQueue(isNull())).thenReturn(null);

    MappingRuleValidationContextImpl ctx =
        new MappingRuleValidationContextImpl(qm);

    //in the beginning there were no variables
    assertEquals(0, ctx.getVariables().size());
    //hence all quese were considered static
    assertTrue(ctx.isPathStatic("root.%user"));
    try {
      //but then suddenly there was a variable
      ctx.addVariable("%user");
      ctx.addVariable("%user");
      assertEquals(1, ctx.getVariables().size());
      //and suddenly previously static queues became dynamic
      assertFalse(ctx.isPathStatic("root.%user"));
      //as time passed, more and more variables joined the void
      ctx.addVariable("%primary_group");
      ctx.addVariable("%default");
    } catch (YarnException e) {
      fail("We don't expect the add variable to fail: " + e.getMessage());
    }
    assertEquals(3, ctx.getVariables().size());
    //making more and more dynamic queues possible
    assertFalse(ctx.isPathStatic("root.%primary_group.something"));
    assertFalse(ctx.isPathStatic("root.something.%default"));
    //but the majority of the queues remained static
    assertTrue(ctx.isPathStatic("root.static"));
    assertTrue(ctx.isPathStatic("root.static.%nothing"));
    assertTrue(ctx.isPathStatic("root"));

    assertTrue(ctx.getVariables().contains("%user"));
    assertTrue(ctx.getVariables().contains("%primary_group"));
    assertTrue(ctx.getVariables().contains("%default"));
    assertFalse(ctx.getVariables().contains("%nothing"));
  }

  void assertValidPath(MappingRuleValidationContext ctx, String path) {
    try {
      ctx.validateQueuePath(path);
    } catch (YarnException e) {
      fail("Path '" + path + "' should be VALID: " + e);
    }
  }

  void assertInvalidPath(MappingRuleValidationContext ctx, String path) {
    try {
      ctx.validateQueuePath(path);
      fail("Path '" + path + "' should be INVALID");
    } catch (YarnException e) {
      //Exception is expected
    }
  }

  @Test
  public void testManagedQueueValidation() {
    //Setting up queue manager and emulated queue hierarchy
    CapacitySchedulerQueueManager qm =
        mock(CapacitySchedulerQueueManager.class);

    MockQueueHierarchyBuilder.create()
        .withQueueManager(qm)
        .withQueue("root.unmanaged")
        .withManagedParentQueue("root.managed")
        .withQueue("root.unmanagedwithchild.child")
        .withQueue("root.leaf")
        .build();
    when(qm.getQueue(isNull())).thenReturn(null);

    MappingRuleValidationContextImpl ctx =
        new MappingRuleValidationContextImpl(qm);
    try {
      ctx.addVariable("%dynamic");
      ctx.addVariable("%user");
    } catch (YarnException e) {
      fail("We don't expect the add variable to fail: " + e.getMessage());
    }

    assertValidPath(ctx, "%dynamic");
    assertValidPath(ctx, "root.%dynamic");
    assertValidPath(ctx, "%user.%dynamic");
    assertValidPath(ctx, "root.managed.%dynamic");
    assertValidPath(ctx, "managed.%dynamic");

    assertInvalidPath(ctx, "root.invalid.%dynamic");
    assertInvalidPath(ctx, "root.unmanaged.%dynamic");

    assertValidPath(ctx, "root.unmanagedwithchild.%user");
    assertValidPath(ctx, "unmanagedwithchild.%user");
  }

  @Test
  public void testDynamicQueueValidation() {
    //Setting up queue manager and emulated queue hierarchy
    CapacitySchedulerQueueManager qm =
        mock(CapacitySchedulerQueueManager.class);

    MockQueueHierarchyBuilder.create()
        .withQueueManager(qm)
        .withQueue("root.unmanaged")
        .withDynamicParentQueue("root.managed")
        .withQueue("root.unmanagedwithchild.child")
        .withQueue("root.leaf")
        .build();
    when(qm.getQueue(isNull())).thenReturn(null);

    MappingRuleValidationContextImpl ctx =
        new MappingRuleValidationContextImpl(qm);
    try {
      ctx.addVariable("%dynamic");
      ctx.addVariable("%user");
    } catch (YarnException e) {
      fail("We don't expect the add variable to fail: " + e.getMessage());
    }

    assertValidPath(ctx, "%dynamic");
    assertValidPath(ctx, "root.%dynamic");
    assertValidPath(ctx, "%user.%dynamic");
    assertValidPath(ctx, "root.managed.%dynamic");
    assertValidPath(ctx, "managed.%dynamic");
    assertValidPath(ctx, "managed.static");
    assertValidPath(ctx, "managed.static.%dynamic");
    assertValidPath(ctx, "managed.static.%dynamic.%dynamic");

    assertInvalidPath(ctx, "root.invalid.%dynamic");
    assertInvalidPath(ctx, "root.unmanaged.%dynamic");

    assertValidPath(ctx, "root.unmanagedwithchild.%user");
    assertValidPath(ctx, "unmanagedwithchild.%user");
  }


  @Test
  public void testStaticQueueValidation() {
    //Setting up queue manager and emulated queue hierarchy
    CapacitySchedulerQueueManager qm =
        mock(CapacitySchedulerQueueManager.class);

    MockQueueHierarchyBuilder.create()
        .withQueueManager(qm)
        .withQueue("root.unmanaged")
        .withManagedParentQueue("root.managed")
        .withQueue("root.deep.queue.path")
        .withQueue("root.ambi.ambileaf")
        .withQueue("root.deep.ambi.ambileaf")
        .withQueue("root.deep.ambi.very.deeepleaf")
        .withDynamicParentQueue("root.dynamic")
        .withQueue("root.dynamic.static.static")
        .build();
    when(qm.getQueue(isNull())).thenReturn(null);

    MappingRuleValidationContextImpl ctx =
        new MappingRuleValidationContextImpl(qm);

    assertValidPath(ctx, "root.unmanaged");
    assertValidPath(ctx, "unmanaged");
    assertInvalidPath(ctx, "root");
    assertInvalidPath(ctx, "managed");
    assertInvalidPath(ctx, "root.managed");
    assertInvalidPath(ctx, "fail");

    assertInvalidPath(ctx, "ambi");
    assertInvalidPath(ctx, "ambileaf");
    assertInvalidPath(ctx, "ambi.ambileaf");
    assertValidPath(ctx, "root.ambi.ambileaf");

    assertInvalidPath(ctx, "root.dynamic.static");
    assertValidPath(ctx, "root.dynamic.static.static");
    //Invalid because static is already created as a non-dynamic parent queue
    assertInvalidPath(ctx, "root.dynamic.static.any");
    //Valid because 'any' is not created yet
    assertValidPath(ctx, "root.dynamic.any.thing");
    //Too deep, dynamic is the last dynamic parent
    assertInvalidPath(ctx, "root.dynamic.any.thing.deep");

    assertValidPath(ctx, "root.managed.a");
    assertInvalidPath(ctx, "root.deep");
    assertInvalidPath(ctx, "deep");
    assertInvalidPath(ctx, "deep.queue");
    assertInvalidPath(ctx, "root.deep.queue");
    assertValidPath(ctx, "deep.queue.path");
    assertInvalidPath(ctx, "ambi.very.deeepleaf");
    assertValidPath(ctx, "queue.path");
    assertInvalidPath(ctx, "queue.invalidPath");
    assertValidPath(ctx, "path");
    assertValidPath(ctx, "root.deep.queue.path");
  }

  @Test
  public void testImmutableVariablesInContext() {
    CapacitySchedulerQueueManager qm =
        mock(CapacitySchedulerQueueManager.class);

    MappingRuleValidationContextImpl ctx =
        new MappingRuleValidationContextImpl(qm);

    try {
      ctx.addVariable("mutable");
      ctx.addVariable("mutable");
    } catch (YarnException e) {
      fail("We should be able to add a mutable variable multiple times: "
          + e.getMessage());
    }

    try {
      ctx.addImmutableVariable("mutable");
      fail("We should receive an exception if an already added mutable" +
          " variable is being marked as immutable");
    } catch (YarnException e) {
      //An exception is expected
    }

    try {
      ctx.addImmutableVariable("immutable");
      ctx.addImmutableVariable("immutable");
    } catch (YarnException e) {
      fail("We should be able to add a immutable variable multiple times: "
          + e.getMessage());
    }

    try {
      ctx.addVariable("immutable");
      fail("We should receive an exception if we try to add a variable as " +
          "mutable when it was previously added as immutable");
    } catch (YarnException e) {
      //An exception is expected
    }
  }
}
