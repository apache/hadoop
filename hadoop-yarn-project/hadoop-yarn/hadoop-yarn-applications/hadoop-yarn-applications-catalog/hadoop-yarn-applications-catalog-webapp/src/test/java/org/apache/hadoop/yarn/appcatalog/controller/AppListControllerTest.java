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

package org.apache.hadoop.yarn.appcatalog.controller;

import org.apache.hadoop.yarn.appcatalog.model.AppEntry;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for AppListController.
 */
public class AppListControllerTest {

  private AppListController controller;

  @Before
  public void setUp() throws Exception {
    this.controller = new AppListController();

  }

  @Test
  public void testGetList() throws Exception {
    AppListController ac = Mockito.mock(AppListController.class);

    List<AppEntry> actual = new ArrayList<AppEntry>();
    when(ac.getList()).thenReturn(actual);
    final List<AppEntry> result = ac.getList();
    assertEquals(result, actual);
  }

  @Test
  public void testDelete() throws Exception {
    String id = "application 1";
    AppListController ac = Mockito.mock(AppListController.class);

    Response expected = Response.ok().build();
    when(ac.delete(id, id)).thenReturn(Response.ok().build());
    final Response actual = ac.delete(id, id);
    assertEquals(expected.getStatus(), actual.getStatus());
  }

  @Test
  public void testDeploy() throws Exception {
    String id = "application 1";
    AppListController ac = Mockito.mock(AppListController.class);
    Service service = new Service();
    Response expected = Response.ok().build();
    when(ac.deploy(id, service)).thenReturn(Response.ok().build());
    final Response actual = ac.deploy(id, service);
    assertEquals(expected.getStatus(), actual.getStatus());
  }

  @Test
  public void testPathAnnotation() throws Exception {
    assertNotNull(this.controller.getClass()
        .getAnnotations());
    assertThat("The controller has the annotation Path",
        this.controller.getClass().isAnnotationPresent(Path.class));

    final Path path = this.controller.getClass()
        .getAnnotation(Path.class);
    assertThat("The path is /app_list", path.value(), is("/app_list"));
  }

}
