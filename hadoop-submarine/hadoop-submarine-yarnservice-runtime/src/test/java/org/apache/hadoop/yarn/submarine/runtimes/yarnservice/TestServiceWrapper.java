/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice;

import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Class to test the {@link ServiceWrapper}.
 */
public class TestServiceWrapper {
  private AbstractComponent createMockAbstractComponent(Component mockComponent,
      String componentName, String localScriptFile) throws IOException {
    when(mockComponent.getName()).thenReturn(componentName);

    AbstractComponent mockAbstractComponent = mock(AbstractComponent.class);
    when(mockAbstractComponent.createComponent()).thenReturn(mockComponent);
    when(mockAbstractComponent.getLocalScriptFile())
        .thenReturn(localScriptFile);
    return mockAbstractComponent;
  }

  @Test
  public void testWithSingleComponent() throws IOException {
    Service mockService = mock(Service.class);
    ServiceWrapper serviceWrapper = new ServiceWrapper(mockService);

    Component mockComponent = mock(Component.class);
    AbstractComponent mockAbstractComponent =
        createMockAbstractComponent(mockComponent, "testComponent",
            "testLocalScriptFile");
    serviceWrapper.addComponent(mockAbstractComponent);

    verify(mockService).addComponent(eq(mockComponent));

    String launchCommand =
        serviceWrapper.getLocalLaunchCommandPathForComponent("testComponent");
    assertEquals("testLocalScriptFile", launchCommand);
  }

  @Test
  public void testWithMultipleComponent() throws IOException {
    Service mockService = mock(Service.class);
    ServiceWrapper serviceWrapper = new ServiceWrapper(mockService);

    Component mockComponent1 = mock(Component.class);
    AbstractComponent mockAbstractComponent1 =
        createMockAbstractComponent(mockComponent1, "testComponent1",
            "testLocalScriptFile1");

    Component mockComponent2 = mock(Component.class);
    AbstractComponent mockAbstractComponent2 =
        createMockAbstractComponent(mockComponent2, "testComponent2",
            "testLocalScriptFile2");

    serviceWrapper.addComponent(mockAbstractComponent1);
    serviceWrapper.addComponent(mockAbstractComponent2);

    verify(mockService).addComponent(eq(mockComponent1));
    verify(mockService).addComponent(eq(mockComponent2));

    String launchCommand1 =
        serviceWrapper.getLocalLaunchCommandPathForComponent("testComponent1");
    assertEquals("testLocalScriptFile1", launchCommand1);

    String launchCommand2 =
        serviceWrapper.getLocalLaunchCommandPathForComponent("testComponent2");
    assertEquals("testLocalScriptFile2", launchCommand2);
  }


}