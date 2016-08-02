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

package org.apache.slider.core.persist;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Files;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.params.AbstractClusterBuildingActionArgs;
import org.apache.slider.common.params.Arguments;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.providers.agent.AgentKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Class to prepare and persist app and add-on definitions.
 *
 * In this case, the app definition and add-on definitions are auto-inferred from the user input rather than explicit
 * inclusion of application package in the config.
 *
 * Processing an app definition involves one or more of the following: - modify appConfig - package definition into a
 * temporary folder - upload to HDFS
 *
 * This class keeps track of all the required operations and allows them to be invoked by build operation
 */
public class AppDefinitionPersister {
  private static final Logger log =
      LoggerFactory.getLogger(AppDefinitionPersister.class);

  private final SliderFileSystem sliderFileSystem;
  private List<AppDefinition> appDefinitions;

  public AppDefinitionPersister(SliderFileSystem sliderFileSystem) {
    this.sliderFileSystem = sliderFileSystem;
    appDefinitions = new ArrayList<>();
  }


  /**
   * Process the application package or folder by copying it to the cluster path
   *
   * @param appDefinition details of application package
   *
   * @throws BadConfigException
   * @throws IOException
   */
  private void persistDefinitionPackageOrFolder(AppDefinition appDefinition)
      throws BadConfigException, IOException {
    if (!appDefinition.appDefPkgOrFolder.canRead()) {
      throw new BadConfigException("Pkg/Folder cannot be accessed - "
                                   + appDefinition.appDefPkgOrFolder.getAbsolutePath());
    }

    File src = appDefinition.appDefPkgOrFolder;
    String targetName = appDefinition.pkgName;
    log.debug("Package name: " + targetName);
    if (appDefinition.appDefPkgOrFolder.isDirectory()) {
      log.info("Processing app package/folder {} for {}",
               appDefinition.appDefPkgOrFolder.getAbsolutePath(),
               appDefinition.pkgName);
      File tmpDir = Files.createTempDir();
      File zipFile = new File(tmpDir.getCanonicalPath(), File.separator + appDefinition.pkgName);
      SliderUtils.zipFolder(appDefinition.appDefPkgOrFolder, zipFile);
      src = zipFile;
    }

    sliderFileSystem.getFileSystem().copyFromLocalFile(
        false,
        false,
        new Path(src.toURI()),
        new Path(appDefinition.targetFolderInFs, targetName));
  }

  public void persistPackages() throws BadConfigException, IOException {
    for (AppDefinition appDefinition : appDefinitions) {
      persistDefinitionPackageOrFolder(appDefinition);
    }
  }

  public void processSuppliedDefinitions(String clustername,
                                         AbstractClusterBuildingActionArgs buildInfo,
                                         ConfTreeOperations appConf)
      throws BadConfigException, IOException, BadCommandArgumentsException {
    // if metainfo is provided add to the app instance
    if (buildInfo.appMetaInfo != null || buildInfo.appMetaInfoJson != null) {
      if (buildInfo.appMetaInfo != null && buildInfo.appMetaInfoJson != null) {
        throw new BadConfigException("Both %s and %s cannot be specified",
            Arguments.ARG_METAINFO, Arguments.ARG_METAINFO_JSON);
      }

      // Now we know that only one of either file or JSON is used
      boolean isFileUsed = buildInfo.appMetaInfo != null ? true : false;
      String argUsed = isFileUsed ? Arguments.ARG_METAINFO
          : Arguments.ARG_METAINFO_JSON;

      if (buildInfo.appDef != null) {
        throw new BadConfigException("Both %s and %s cannot be specified",
            argUsed, Arguments.ARG_APPDEF);
      }
      if (SliderUtils.isSet(appConf.getGlobalOptions().get(AgentKeys.APP_DEF))) {
        throw new BadConfigException(
            "%s cannot not be set if %s is specified in the cmd line ",
            AgentKeys.APP_DEF, argUsed);
      }

      if (isFileUsed) {
        if (!buildInfo.appMetaInfo.canRead() || !buildInfo.appMetaInfo.isFile()) {
          throw new BadConfigException(
              "Path specified with %s either cannot be read or is not a file",
              Arguments.ARG_METAINFO);
        }
      } else {
        if (StringUtils.isEmpty(buildInfo.appMetaInfoJson.trim())) {
          throw new BadConfigException("Empty string specified with %s",
              Arguments.ARG_METAINFO_JSON);
        }
      }

      File tempDir = Files.createTempDir();
      File pkgSrcDir = new File(tempDir, "default");
      pkgSrcDir.mkdirs();
      File destMetaInfo = new File(pkgSrcDir, "metainfo.json");
      if (isFileUsed) {
        if (buildInfo.appMetaInfo.getName().endsWith(".xml")) {
          Files.copy(buildInfo.appMetaInfo, new File(pkgSrcDir, "metainfo.xml"));
        } else {
          Files.copy(buildInfo.appMetaInfo, destMetaInfo);
        }
      } else {
        Files.write(
            buildInfo.appMetaInfoJson.getBytes(Charset.forName("UTF-8")),
            destMetaInfo);
      }

      Path appDirPath = sliderFileSystem.buildAppDefDirPath(clustername);
      log.info("Using default app def path {}", appDirPath.toString());

      appDefinitions.add(new AppDefinition(appDirPath, pkgSrcDir,
          SliderKeys.DEFAULT_APP_PKG));
      Path appDefPath = new Path(appDirPath, SliderKeys.DEFAULT_APP_PKG);
      appConf.getGlobalOptions().set(AgentKeys.APP_DEF, appDefPath);
      log.info("Setting app package to {}.", appDefPath);
    }

    if (buildInfo.appDef != null) {
      if (SliderUtils.isSet(appConf.getGlobalOptions().get(AgentKeys.APP_DEF))) {
        throw new BadConfigException("application.def must not be set if --appdef is provided.");
      }

      if (!buildInfo.appDef.exists()) {
        throw new BadConfigException("--appdef is not a valid path.");
      }

      Path appDirPath = sliderFileSystem.buildAppDefDirPath(clustername);
      appDefinitions.add(new AppDefinition(appDirPath, buildInfo.appDef, SliderKeys.DEFAULT_APP_PKG));
      Path appDefPath = new Path(appDirPath, SliderKeys.DEFAULT_APP_PKG);
      appConf.getGlobalOptions().set(AgentKeys.APP_DEF, appDefPath);
      log.info("Setting app package to {}.", appDefPath);
    }

    if (buildInfo.addonDelegate.getAddonMap().size() > 0) {
      if (SliderUtils.isUnset(appConf.getGlobalOptions().get(AgentKeys.APP_DEF))) {
        throw new BadConfigException("addon package can only be specified if main app package is specified.");
      }

      List<String> addons = new ArrayList<String>();
      Map<String, String> addonMap = buildInfo.addonDelegate.getAddonMap();
      for (String key : addonMap.keySet()) {
        File defPath = new File(addonMap.get(key));
        if (SliderUtils.isUnset(addonMap.get(key))) {
          throw new BadConfigException("Invalid path for addon package " + key);
        }

        if (!defPath.exists()) {
          throw new BadConfigException("addon folder or package path is not valid.");
        }

        Path addonPath = sliderFileSystem.buildAddonDirPath(clustername, key);
        String addonPkgName = "addon_" + key + ".zip";

        log.debug(
            "addonMap.get(key): {} addonPath: {} defPath: {} addonPkgName: {}",
            addonMap.get(key), addonPath, defPath, addonPkgName);

        appDefinitions.add(new AppDefinition(addonPath, defPath, addonPkgName));
        String addOnKey = AgentKeys.ADDON_PREFIX + key;
        Path addonPkgPath = new Path(addonPath, addonPkgName);
        log.info("Setting addon package {} to {}.", addOnKey, addonPkgPath);
        appConf.getGlobalOptions().set(addOnKey, addonPkgPath);
        addons.add(addOnKey);
      }

      String existingList = appConf.getGlobalOptions().get(AgentKeys.ADDONS);
      if (SliderUtils.isUnset(existingList)) {
        existingList = "";
      }
      appConf.getGlobalOptions().set(AgentKeys.ADDONS, existingList + StringUtils.join(addons, ","));
    }
  }


  @VisibleForTesting
  public List<AppDefinitionPersister.AppDefinition> getAppDefinitions() {
    return appDefinitions;
  }

  // Helper class to hold details for the app and addon packages
  public class AppDefinition {
    // The target folder where the package will be stored
    public Path targetFolderInFs;
    // The on disk location of the app def package or folder
    public File appDefPkgOrFolder;
    // Package name
    public String pkgName;

    public AppDefinition(Path targetFolderInFs, File appDefPkgOrFolder, String pkgName) {
      this.targetFolderInFs = targetFolderInFs;
      this.appDefPkgOrFolder = appDefPkgOrFolder;
      this.pkgName = pkgName;
    }

    @Override
    public String toString() {
      return new StringBuilder().append("targetFolderInFs").append(" : ").append(targetFolderInFs.toString())
          .append(", ")
          .append("appDefPkgOrFolder").append(" : ").append(appDefPkgOrFolder.toString())
          .append(", ")
          .append("pkgName").append(" : ").append(pkgName).toString();
    }
  }
}
