/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.hamlet.HamletImpl.EImp;
import org.apache.slider.api.ClusterNode;
import org.apache.slider.providers.ProviderService;
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockContainer;
import org.apache.slider.server.appmaster.model.mock.MockContainerId;
import org.apache.slider.server.appmaster.model.mock.MockNodeId;
import org.apache.slider.server.appmaster.model.mock.MockProviderService;
import org.apache.slider.server.appmaster.model.mock.MockResource;
import org.apache.slider.server.appmaster.state.ProviderAppState;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.apache.slider.server.appmaster.web.WebAppApiImpl;
import org.apache.slider.server.appmaster.web.view.ContainerStatsBlock.ClusterNodeNameComparator;
import org.apache.slider.server.appmaster.web.view.ContainerStatsBlock.TableAnchorContent;
import org.apache.slider.server.appmaster.web.view.ContainerStatsBlock.TableContent;
import org.junit.Before;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test container stats block.
 */
public class TestContainerStatsBlock extends BaseMockAppStateTest {

  private ContainerStatsBlock statsBlock;

  private Container cont1, cont2;

  @Before
  public void setup() throws Exception {
    super.setup();
    ProviderService providerService = new MockProviderService();
    ProviderAppState providerAppState = new ProviderAppState(
        "undefined",
        appState);

    WebAppApiImpl inst = new WebAppApiImpl(
        providerAppState,
        providerService,
        null,
        METRICS, null);

    Injector injector = Guice.createInjector(new WebappModule(inst));
    statsBlock = injector.getInstance(ContainerStatsBlock.class);

    cont1 = new MockContainer();

    cont1.setId(mockContainerId(0));
    cont1.setNodeId(new MockNodeId());
    cont1.setPriority(Priority.newInstance(1));
    cont1.setResource(new MockResource(0, 0));

    cont2 = new MockContainer();
    cont2.setId(mockContainerId(1));
    cont2.setNodeId(new MockNodeId());
    cont2.setPriority(Priority.newInstance(1));
    cont2.setResource(new MockResource(0, 0));
  }

  private static class WebappModule extends AbstractModule {
    private final WebAppApiImpl instance;

    WebappModule(WebAppApiImpl instance) {
      this.instance = instance;
    }

    @Override
    protected void configure() {
      bind(WebAppApi.class).toInstance(instance);
    }
  }


  public MockContainerId mockContainerId(int count) {
    return new MockContainerId(applicationAttemptId, count);
  }

  @Test
  public void testGetContainerInstances() {
    List<RoleInstance> roles = Arrays.asList(
        new RoleInstance(cont1),
        new RoleInstance(cont2)
    );
    Map<String, RoleInstance> map = statsBlock.getContainerInstances(roles);

    assertEquals(2, map.size());

    assertTrue(map.containsKey("mockcontainer_0"));
    assertEquals(map.get("mockcontainer_0"), roles.get(0));

    assertTrue(map.containsKey("mockcontainer_1"));
    assertEquals(map.get("mockcontainer_1"), roles.get(1));
  }

  @Test
  public void testGenerateRoleDetailsWithTwoColumns() {
    StringWriter sw = new StringWriter(64);
    PrintWriter pw = new PrintWriter(sw);

    Hamlet hamlet = new Hamlet(pw, 0, false);

    // Make a div to put the content into
    DIV<Hamlet> div = hamlet.div();

    String detailsName = "testing";
    String selector = "selector";
    Map<TableContent, String> data = new HashMap<>();
    data.put(new ContainerStatsBlock.TableContent("Foo"), "bar");

    int levelPrior = hamlet.nestLevel();
    statsBlock.generateRoleDetails(div, selector, detailsName, data.entrySet());

    // Close out the div we made
    // DIV<Hamlet>._() will actually invoke the wrong method (creating <p>),
    // explicit cast to make sure we're closing out the <div>
    ((EImp) div)._();

    assertEquals(levelPrior, hamlet.nestLevel());
  }

  @Test
  public void testGenerateRoleDetailsWithOneColumn() {
    StringWriter sw = new StringWriter(64);
    PrintWriter pw = new PrintWriter(sw);

    Hamlet hamlet = new Hamlet(pw, 0, false);
    DIV<Hamlet> div = hamlet.div();

    String detailsName = "testing";
    String selector = "selector";
    Map<TableContent, String> data = new HashMap<>();
    data.put(new ContainerStatsBlock.TableContent("Bar"), null);

    int levelPrior = hamlet.nestLevel();
    statsBlock.generateRoleDetails(div, selector, detailsName, data.entrySet());

    // Close out the div we made
    // DIV<Hamlet>._() will actually invoke the wrong method (creating <p>),
    // explicit cast to make sure we're closing out the <div>
    ((EImp) div)._();

    assertEquals(levelPrior, hamlet.nestLevel());
  }

  @Test
  public void testGenerateRoleDetailsWithNoData() {
    StringWriter sw = new StringWriter(64);
    PrintWriter pw = new PrintWriter(sw);

    Hamlet hamlet = new Hamlet(pw, 0, false);
    DIV<Hamlet> div = hamlet.div();

    String detailsName = "testing";
    String selector = "selector";
    Map<TableContent, String> data = new HashMap<>();

    int levelPrior = hamlet.nestLevel();
    statsBlock.generateRoleDetails(div, selector, detailsName, data.entrySet());

    // Close out the div we made
    // DIV<Hamlet>._() will actually invoke the wrong method (creating <p>),
    // explicit cast to make sure we're closing out the <div>
    ((EImp) div)._();

    assertEquals(levelPrior, hamlet.nestLevel());
  }

  @Test
  public void testClusterNodeNameComparator() {
    ClusterNode n1 = new ClusterNode(mockContainerId(1)),
        n2 = new ClusterNode(mockContainerId(2)),
        n3 = new ClusterNode(mockContainerId(3));

    List<ClusterNode> nodes = new ArrayList<ClusterNode>();
    nodes.add(n2);
    nodes.add(n3);
    nodes.add(n1);

    Collections.sort(nodes, new ClusterNodeNameComparator());

    String prevName = "";
    for (ClusterNode node : nodes) {
      assertTrue(prevName.compareTo(node.name) <= 0);
      prevName = node.name;
    }
  }

  @Test
  public void testTableContent() {
    StringWriter sw = new StringWriter(64);
    PrintWriter pw = new PrintWriter(sw);
    TableContent tc = new TableContent("foo");

    Hamlet hamlet = new Hamlet(pw, 0, false);
    TR<TABLE<Hamlet>> tr = hamlet.table().tr();

    int prevLevel = hamlet.nestLevel();
    // printCell should not end the tr
    tc.printCell(tr);
    tr._();
    assertEquals(prevLevel, hamlet.nestLevel());
  }

  @Test
  public void testTableAnchorContent() {
    StringWriter sw = new StringWriter(64);
    PrintWriter pw = new PrintWriter(sw);
    TableContent tc = new TableAnchorContent("foo", "http://bar.com");

    Hamlet hamlet = new Hamlet(pw, 0, false);
    TR<TABLE<Hamlet>> tr = hamlet.table().tr();

    int prevLevel = hamlet.nestLevel();
    // printCell should not end the tr
    tc.printCell(tr);
    tr._();
    assertEquals(prevLevel, hamlet.nestLevel());
  }
}
