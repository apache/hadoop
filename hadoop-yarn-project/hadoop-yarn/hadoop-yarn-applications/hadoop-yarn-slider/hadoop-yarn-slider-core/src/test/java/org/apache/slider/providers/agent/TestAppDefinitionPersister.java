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
package org.apache.slider.providers.agent;

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.slider.common.params.ActionCreateArgs;
import org.apache.slider.common.params.AddonArgsDelegate;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.persist.AppDefinitionPersister;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class TestAppDefinitionPersister {
  protected static final Logger log =
      LoggerFactory.getLogger(TestAppDefinitionPersister.class);
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  /**
   * @BeforeClass public static void initialize() { BasicConfigurator.resetConfiguration();
   * BasicConfigurator.configure(); }*
   */


  @Test
  public void testAppDefinitionPersister() throws Exception {
    Configuration configuration = new Configuration();
    FileSystem fs = FileSystem.getLocal(configuration);
    log.info("fs working dir is {}", fs.getWorkingDirectory().toString());
    SliderFileSystem sliderFileSystem = new SliderFileSystem(fs, configuration);

    AppDefinitionPersister adp = new AppDefinitionPersister(sliderFileSystem);
    String clustername = "c1";
    ActionCreateArgs buildInfo = new ActionCreateArgs();
    buildInfo.appMetaInfo = null;
    buildInfo.appDef = null;
    buildInfo.addonDelegate = new AddonArgsDelegate();

    // nothing to do
    adp.processSuppliedDefinitions(clustername, buildInfo, null);
    adp.persistPackages();
    List<AppDefinitionPersister.AppDefinition> appDefinitions = adp.getAppDefinitions();
    Assert.assertTrue(appDefinitions.size() == 0);

    ConfTree ct = new ConfTree();
    ConfTreeOperations appConf = new ConfTreeOperations(ct);
    final File tempDir = Files.createTempDir();
    final File metainfo = new File(tempDir, "metainfo.json");

    // unreadable metainfo
    buildInfo.appMetaInfo = metainfo;

    try {
      adp.processSuppliedDefinitions(clustername, buildInfo, appConf);
    } catch (BadConfigException bce) {
      log.info(bce.getMessage());
      Assert.assertTrue(bce.getMessage().contains(
          "Path specified with "
              + "--metainfo either cannot be read or is not a file"));
    }

    try (PrintWriter writer = new PrintWriter(metainfo.getAbsolutePath(), "UTF-8")) {
      writer.println("{");
      writer.println("}");
    }
    buildInfo.appDef = metainfo;

    try {
      adp.processSuppliedDefinitions(clustername, buildInfo, appConf);
    } catch (BadConfigException bce) {
      log.info(bce.getMessage());
      Assert.assertTrue(bce.getMessage().contains(
          "Both --metainfo and --appdef cannot be specified"));
    }

    // both --metainfojson and --appdef cannot be specified
    buildInfo.appMetaInfo = null;
    buildInfo.appMetaInfoJson = "{}";
    try {
      adp.processSuppliedDefinitions(clustername, buildInfo, appConf);
    } catch (BadConfigException bce) {
      log.info(bce.getMessage());
      Assert.assertTrue(bce.getMessage().contains(
          "Both --metainfojson and --appdef cannot be specified"));
    }

    buildInfo.appDef = null;

    buildInfo.appMetaInfoJson = "";
    try {
      adp.processSuppliedDefinitions(clustername, buildInfo, appConf);
    } catch (BadConfigException bce) {
      log.info(bce.getMessage());
      Assert.assertTrue(bce.getMessage().contains(
          "Empty string specified with --metainfojson"));
    }
    buildInfo.appMetaInfo = metainfo;

    // both --metainfo and --metainfojson cannot be specified
    buildInfo.appMetaInfoJson = "{}";
    try {
      adp.processSuppliedDefinitions(clustername, buildInfo, appConf);
    } catch (BadConfigException bce) {
      log.info(bce.getMessage());
      Assert.assertTrue(bce.getMessage().contains(
          "Both --metainfo and --metainfojson cannot be specified"));
    }
    buildInfo.appMetaInfoJson = null;

    appConf.getGlobalOptions().set(AgentKeys.APP_DEF, metainfo.getAbsolutePath());

    try {
      adp.processSuppliedDefinitions(clustername, buildInfo, appConf);
    } catch (BadConfigException bce) {
      log.info(bce.getMessage());
      Assert.assertTrue(bce.getMessage().contains(
          "application.def cannot "
              + "not be set if --metainfo is specified in the cmd line"));
    }

    appConf.getGlobalOptions().remove(AgentKeys.APP_DEF);

    adp.processSuppliedDefinitions(clustername, buildInfo, appConf);
    appDefinitions = adp.getAppDefinitions();
    Assert.assertTrue(appDefinitions.size() == 1);
    Assert.assertTrue(appConf.getGlobalOptions().get(AgentKeys.APP_DEF).contains("appdef/appPkg.zip"));
    log.info(appDefinitions.get(0).toString());
    Assert.assertTrue(appDefinitions.get(0).appDefPkgOrFolder.toString().endsWith("default"));
    Assert.assertTrue(appDefinitions.get(0).targetFolderInFs.toString().contains("cluster/c1/appdef"));
    Assert.assertEquals("appPkg.zip", appDefinitions.get(0).pkgName);

    buildInfo.appDef = tempDir;
    buildInfo.appMetaInfo = null;

    appConf.getGlobalOptions().set(AgentKeys.APP_DEF, metainfo.getAbsolutePath());

    try {
      adp.processSuppliedDefinitions(clustername, buildInfo, appConf);
    } catch (BadConfigException bce) {
      log.info(bce.getMessage());
      Assert.assertTrue(bce.getMessage().contains("application.def must not be set if --appdef is provided"));
    }

    adp.getAppDefinitions().clear();
    appConf.getGlobalOptions().remove(AgentKeys.APP_DEF);
    adp.processSuppliedDefinitions(clustername, buildInfo, appConf);
    appDefinitions = adp.getAppDefinitions();
    Assert.assertTrue(appDefinitions.size() == 1);
    Assert.assertTrue(appConf.getGlobalOptions().get(AgentKeys.APP_DEF).contains("appdef/appPkg.zip"));
    log.info(appDefinitions.get(0).toString());
    Assert.assertTrue(appDefinitions.get(0).appDefPkgOrFolder.toString().endsWith(tempDir.toString()));
    Assert.assertTrue(appDefinitions.get(0).targetFolderInFs.toString().contains("cluster/c1/appdef"));
    Assert.assertEquals("appPkg.zip", appDefinitions.get(0).pkgName);

    adp.getAppDefinitions().clear();
    buildInfo.appDef = null;
    buildInfo.appMetaInfo = null;
    appConf.getGlobalOptions().remove(AgentKeys.APP_DEF);

    ArrayList<String> list = new ArrayList<String>() {{
      add("addon1");
      add("");
      add("addon2");
      add(metainfo.getAbsolutePath());
    }};

    buildInfo.addonDelegate.addonTuples = list;
    try {
      adp.processSuppliedDefinitions(clustername, buildInfo, appConf);
    } catch (BadConfigException bce) {
      log.info(bce.getMessage());
      Assert.assertTrue(bce.getMessage().contains("addon package can only be specified if main app package is specified"));
    }

    buildInfo.appMetaInfo = metainfo;

    try {
      adp.processSuppliedDefinitions(clustername, buildInfo, appConf);
    } catch (BadConfigException bce) {
      log.info(bce.getMessage());
      Assert.assertTrue(bce.getMessage().contains("Invalid path for addon package addon1"));
    }

    appConf.getGlobalOptions().remove(AgentKeys.APP_DEF);

    list = new ArrayList<String>() {{
      add("addon1");
      add(tempDir.getAbsolutePath());
      add("addon2");
      add(metainfo.getAbsolutePath());
    }};

    buildInfo.addonDelegate.addonTuples = list;
    adp.getAppDefinitions().clear();

    adp.processSuppliedDefinitions(clustername, buildInfo, appConf);
    appDefinitions = adp.getAppDefinitions();

    Assert.assertTrue(appDefinitions.size() == 3);
    Assert.assertTrue(appConf.getGlobalOptions().get(AgentKeys.APP_DEF).contains("appdef/appPkg.zip"));
    Assert.assertTrue(appConf.getGlobalOptions().get("application.addon.addon1").contains(
        "addons/addon1/addon_addon1.zip"));
    Assert.assertTrue(appConf.getGlobalOptions().get("application.addon.addon2").contains(
        "addons/addon2/addon_addon2.zip"));
    log.info(appConf.getGlobalOptions().get("application.addons"));
    Assert.assertTrue(appConf.getGlobalOptions().get("application.addons").contains(
        "application.addon.addon2,application.addon.addon1")
                      || appConf.getGlobalOptions().get("application.addons").contains(
        "application.addon.addon1,application.addon.addon2"));
    int seen = 0;
    for (AppDefinitionPersister.AppDefinition adp_ad : appDefinitions) {
      if (adp_ad.pkgName.equals("appPkg.zip")) {
        log.info(adp_ad.toString());
        Assert.assertTrue(adp_ad.appDefPkgOrFolder.toString().endsWith("default"));
        Assert.assertTrue(adp_ad.targetFolderInFs.toString().contains("cluster/c1/appdef"));
        seen++;
      }
      if (adp_ad.pkgName.equals("addon_addon1.zip")) {
        log.info(adp_ad.toString());
        Assert.assertTrue(adp_ad.appDefPkgOrFolder.toString().endsWith(tempDir.toString()));
        Assert.assertTrue(adp_ad.targetFolderInFs.toString().contains("addons/addon1"));
        seen++;
      }
      if (adp_ad.pkgName.equals("addon_addon2.zip")) {
        log.info(adp_ad.toString());
        Assert.assertTrue(adp_ad.appDefPkgOrFolder.toString().endsWith("metainfo.json"));
        Assert.assertTrue(adp_ad.targetFolderInFs.toString().contains("addons/addon2"));
        seen++;
      }
    }
    Assert.assertEquals(3, seen);
  }
}
