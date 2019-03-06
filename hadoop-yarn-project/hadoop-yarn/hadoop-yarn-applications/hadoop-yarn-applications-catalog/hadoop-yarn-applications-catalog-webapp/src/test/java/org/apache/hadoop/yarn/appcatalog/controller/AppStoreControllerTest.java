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

import org.apache.hadoop.yarn.appcatalog.model.AppStoreEntry;
import org.apache.hadoop.yarn.appcatalog.model.Application;
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
 * Unit tests for AppStoreController.
 */
public class AppStoreControllerTest {

  private AppStoreController controller;

  @Before
  public void setUp() throws Exception {
    this.controller = new AppStoreController();

  }

  @Test
  public void testGetRecommended() throws Exception {
    AppStoreController ac = Mockito.mock(AppStoreController.class);
    List<AppStoreEntry> actual = new ArrayList<AppStoreEntry>();
    when(ac.get()).thenReturn(actual);
    final List<AppStoreEntry> result = ac.get();
    assertEquals(result, actual);
  }

  @Test
  public void testSearch() throws Exception {
    String keyword = "jenkins";
    AppStoreController ac = Mockito.mock(AppStoreController.class);
    List<AppStoreEntry> expected = new ArrayList<AppStoreEntry>();
    when(ac.search(keyword)).thenReturn(expected);
    final List<AppStoreEntry> actual = ac.search(keyword);
    assertEquals(expected, actual);
  }

  @Test
  public void testRegister() throws Exception {
    AppStoreController ac = Mockito.mock(AppStoreController.class);
    Application app = new Application();
    app.setName("jenkins");
    app.setOrganization("jenkins.org");
    app.setDescription("This is a description");
    app.setIcon("/css/img/feather.png");
    Response expected = Response.ok().build();
    when(ac.register(app)).thenReturn(Response.ok().build());
    final Response actual = ac.register(app);
    assertEquals(expected.getStatus(), actual.getStatus());
  }

  @Test
  public void testPathAnnotation() throws Exception {
    assertNotNull(this.controller.getClass()
        .getAnnotations());
    assertThat("The controller has the annotation Path",
        this.controller.getClass()
        .isAnnotationPresent(Path.class));

    final Path path = this.controller.getClass()
        .getAnnotation(Path.class);
    assertThat("The path is /app_store", path.value(), is("/app_store"));
  }

}
