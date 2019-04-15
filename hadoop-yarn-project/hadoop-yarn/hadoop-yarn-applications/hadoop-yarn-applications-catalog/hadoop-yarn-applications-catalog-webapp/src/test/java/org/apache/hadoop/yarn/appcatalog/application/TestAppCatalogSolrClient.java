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

package org.apache.hadoop.yarn.appcatalog.application;

import org.apache.hadoop.yarn.appcatalog.model.AppEntry;
import org.apache.hadoop.yarn.appcatalog.model.AppStoreEntry;
import org.apache.hadoop.yarn.appcatalog.model.Application;
import org.apache.solr.client.solrj.SolrClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.support.membermodification.MemberMatcher.method;

import static org.junit.Assert.*;

import java.util.List;

/**
 * Unit test for AppCatalogSolrClient.
 */
public class TestAppCatalogSolrClient {

  static final String CONFIGSET_DIR = "src/test/resources/configsets";
  private static SolrClient solrClient;
  private static AppCatalogSolrClient spy;

  @Before
  public void setup() throws Exception {
    String targetLocation = EmbeddedSolrServerFactory.class
        .getProtectionDomain().getCodeSource().getLocation().getFile() + "/..";

    String solrHome = targetLocation + "/solr";
    solrClient = EmbeddedSolrServerFactory.create(solrHome, CONFIGSET_DIR,
        "exampleCollection");
    spy = PowerMockito.spy(new AppCatalogSolrClient());
    when(spy, method(AppCatalogSolrClient.class, "getSolrClient"))
        .withNoArguments().thenReturn(solrClient);
  }

  @After
  public void teardown() throws Exception {
    try {
      solrClient.close();
    } catch (Exception e) {
    }
  }

  @Test
  public void testRegister() throws Exception {
    Application example = new Application();
    example.setOrganization("jenkins-ci.org");
    example.setName("jenkins");
    example.setDescription("World leading open source automation system.");
    example.setIcon("/css/img/feather.png");
    spy.register(example);
    List<AppStoreEntry> apps = spy.getRecommendedApps();
    assertEquals(1, apps.size());
  }

  @Test
  public void testSearch() throws Exception {
    Application example = new Application();
    example.setOrganization("jenkins-ci.org");
    example.setName("jenkins");
    example.setDescription("World leading open source automation system.");
    example.setIcon("/css/img/feather.png");
    spy.register(example);
    List<AppStoreEntry> results = spy.search("name_s:jenkins");
    int expected = 1;
    int actual = results.size();
    assertEquals(expected, actual);
  }

  @Test
  public void testNotFoundSearch() throws Exception {
    Application example = new Application();
    example.setOrganization("jenkins-ci.org");
    example.setName("jenkins");
    example.setDescription("World leading open source automation system.");
    example.setIcon("/css/img/feather.png");
    spy.register(example);
    List<AppStoreEntry> results = spy.search("name_s:abc");
    int expected = 0;
    int actual = results.size();
    assertEquals(expected, actual);
  }

  @Test
  public void testGetRecommendedApps() throws Exception {
    AppStoreEntry example = new AppStoreEntry();
    example.setOrg("jenkins-ci.org");
    example.setName("jenkins");
    example.setDesc("World leading open source automation system.");
    example.setIcon("/css/img/feather.png");
    example.setDownload(100);
    spy.register(example);
    AppStoreEntry example2 = new AppStoreEntry();
    example2.setOrg("Apache");
    example2.setName("httpd");
    example2.setDesc("Apache webserver");
    example2.setIcon("/css/img/feather.png");
    example2.setDownload(1);
    spy.register(example2);
    List<AppStoreEntry> actual = spy.getRecommendedApps();
    long previous = 1000L;
    for (AppStoreEntry app: actual) {
      assertTrue("Recommend app is not sort by download count.",
          previous > app.getDownload());
      previous = app.getDownload();
    }
  }

  @Test
  public void testUpgradeApp() throws Exception {
    Application example = new Application();
    String expected = "2.0";
    String actual = "";
    example.setOrganization("jenkins-ci.org");
    example.setVersion("1.0");
    example.setName("jenkins");
    example.setDescription("World leading open source automation system.");
    example.setIcon("/css/img/feather.png");
    spy.register(example);
    spy.deployApp("test", example);
    example.setVersion("2.0");
    spy.upgradeApp(example);
    List<AppEntry> appEntries = spy.listAppEntries();
    actual = appEntries.get(appEntries.size() -1).getYarnfile().getVersion();
    assertEquals(expected, actual);
  }
}
