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
import org.apache.hadoop.yarn.server.resourcemanager.placement.VariableContext;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestMappingRuleActions {
  void assertRejectResult(MappingRuleResult result) {
    assertSame(MappingRuleResultType.REJECT, result.getResult());
  }


  void assertSkipResult(MappingRuleResult result) {
    assertSame(MappingRuleResultType.SKIP, result.getResult());
  }

  void assertPlaceDefaultResult(MappingRuleResult result) {
    assertSame(MappingRuleResultType.PLACE_TO_DEFAULT, result.getResult());
  }

  void assertPlaceResult(MappingRuleResult result, String queue) {
    assertSame(MappingRuleResultType.PLACE, result.getResult());
    assertEquals(queue, result.getQueue());
  }

  @Test
  public void testRejectAction() {
    VariableContext variables = new VariableContext();
    MappingRuleAction reject = new MappingRuleActions.RejectAction();
    MappingRuleAction rejectHelper = MappingRuleActions.createRejectAction();

    assertRejectResult(reject.execute(variables));
    assertRejectResult(rejectHelper.execute(variables));
  }

  @Test
  public void testActionFallbacks() {
    MappingRuleActionBase action =
        new MappingRuleActions.PlaceToQueueAction("a", true);

    action.setFallbackDefaultPlacement();
    assertPlaceDefaultResult(action.getFallback());

    action.setFallbackReject();
    assertRejectResult(action.getFallback());

    action.setFallbackSkip();
    assertSkipResult(action.getFallback());
  }

  @Test
  public void testVariableUpdateAction() {
    VariableContext variables = new VariableContext();
    variables.put("%default", "root.default");
    variables.put("%immutable", "immutable");
    variables.put("%empty", "");
    variables.put("%null", null);
    variables.put("%sub", "xxx");
    variables.setImmutables("%immutable");

    MappingRuleAction updateDefaultManual =
        new MappingRuleActions.VariableUpdateAction("%default", "root.%sub");
    MappingRuleAction updateDefaultHelper =
        MappingRuleActions.createUpdateDefaultAction("root.%sub%sub");
    MappingRuleAction updateImmutable =
        new MappingRuleActions.VariableUpdateAction("%immutable", "changed");
    MappingRuleAction updateEmpty =
        new MappingRuleActions.VariableUpdateAction("%empty", "something");
    MappingRuleAction updateNull =
        new MappingRuleActions.VariableUpdateAction("%null", "non-null");

    MappingRuleResult result;

    result = updateDefaultManual.execute(variables);
    assertSkipResult(result);
    assertEquals("root.xxx", variables.get("%default"));

    result = updateDefaultHelper.execute(variables);
    assertSkipResult(result);
    assertEquals("root.xxxxxx", variables.get("%default"));

    result = updateEmpty.execute(variables);
    assertSkipResult(result);
    assertEquals("something", variables.get("%empty"));
    result = updateNull.execute(variables);
    assertSkipResult(result);
    assertEquals("non-null", variables.get("%null"));

    try {
      updateImmutable.execute(variables);
      fail("Should've failed with exception");
    } catch (Exception e){
      assertTrue(e instanceof IllegalStateException);
    }
  }

  @Test
  public void testPlaceToQueueAction() {
    VariableContext variables = new VariableContext();
    variables.put("%default", "root.default");
    variables.put("%immutable", "immutable");
    variables.put("%empty", "");
    variables.put("%null", null);
    variables.put("%sub", "xxx");
    variables.setImmutables("%immutable");

    MappingRuleAction placeToStatic =
        new MappingRuleActions.PlaceToQueueAction("root.static.queue", true);

    MappingRuleAction placeToDynamic =
        new MappingRuleActions.PlaceToQueueAction("root.%sub.%immutable", true);

    MappingRuleAction placeToDynamicDoubleSub =
        MappingRuleActions.createPlaceToQueueAction(
            "root.%sub%sub.%immutable", true);

    MappingRuleAction placeToNull =
        MappingRuleActions.createPlaceToQueueAction(null, true);

    MappingRuleAction placeToEmpty =
        MappingRuleActions.createPlaceToQueueAction("", true);

    MappingRuleAction placeToNulRef =
        new MappingRuleActions.PlaceToQueueAction("%null", true);

    MappingRuleAction placeToEmptyRef =
        new MappingRuleActions.PlaceToQueueAction("%empty", true);

    MappingRuleAction placeToDefaultRef =
        new MappingRuleActions.PlaceToQueueAction("%default", true);

    assertPlaceResult(placeToStatic.execute(variables), "root.static.queue");
    assertPlaceResult(placeToDynamic.execute(variables), "root.xxx.immutable");
    assertPlaceResult(placeToDynamicDoubleSub.execute(variables),
        "root.%sub%sub.immutable");

    assertPlaceResult(placeToNull.execute(variables), "");
    assertPlaceResult(placeToEmpty.execute(variables), "");
    assertPlaceResult(placeToNulRef.execute(variables), "");
    assertPlaceResult(placeToEmptyRef.execute(variables), "");
    assertPlaceResult(placeToDefaultRef.execute(variables), "root.default");
  }

  @Test
  public void testToStrings() {
    MappingRuleAction place = new MappingRuleActions.PlaceToQueueAction(
        "queue", true);
    MappingRuleAction varUpdate = new MappingRuleActions.VariableUpdateAction(
        "%var", "value");
    MappingRuleAction reject = new MappingRuleActions.RejectAction();

    assertEquals("PlaceToQueueAction{queueName='queue',allowCreate=true}",
        place.toString());
    assertEquals("VariableUpdateAction{variableName='%var'" +
        ", variableValue='value'}", varUpdate.toString());
    assertEquals("RejectAction", reject.toString());
  }
}