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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.providers.agent.application.metadata.AbstractMetainfoParser;
import org.apache.slider.providers.agent.application.metadata.AddonPackageMetainfoParser;
import org.apache.slider.providers.agent.application.metadata.DefaultConfig;
import org.apache.slider.providers.agent.application.metadata.DefaultConfigParser;
import org.apache.slider.providers.agent.application.metadata.Metainfo;
import org.apache.slider.providers.agent.application.metadata.MetainfoParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import static org.apache.slider.api.RoleKeys.ROLE_PREFIX;

/**
 *
 */
public class AgentUtils {
  private static final Logger log = LoggerFactory.getLogger(AgentUtils.class);

  public static Metainfo getApplicationMetainfoFromSummaryFile(
      SliderFileSystem fileSystem, String metainfoPath, boolean metainfoForAddon) {
    FileSystem fs = fileSystem.getFileSystem();
    Path appPathXML = new Path(metainfoPath + ".metainfo.xml");
    Path appPathJson = new Path(metainfoPath + ".metainfo.json");
    Path appPathUsed = null;
    try {
      FSDataInputStream appStream = null;
      if (fs.exists(appPathXML)) {
        appPathUsed = appPathXML;
        appStream = fs.open(appPathXML);
        return parseMetainfo(appStream, metainfoForAddon, "xml");
      } else if (fs.exists(appPathJson)) {
        appPathUsed = appPathJson;
        appStream = fs.open(appPathJson);
        return parseMetainfo(appStream, metainfoForAddon, "json");
      }
    } catch (IOException e) {
      log.info("Failed to get metainfo from summary file {} - {}", appPathUsed,
          e.getMessage());
      log.debug("Failed to get metainfo", e);
    }
    return null;
  }

  public static Metainfo getApplicationMetainfo(SliderFileSystem fileSystem,
      String metainfoPath, boolean metainfoForAddon) throws IOException,
      BadConfigException {
    log.info("Reading metainfo at {}", metainfoPath);
    Metainfo metainfo = getApplicationMetainfoFromSummaryFile(fileSystem,
        metainfoPath, metainfoForAddon);
    if (metainfo != null) {
      log.info("Got metainfo from summary file");
      return metainfo;
    }

    FileSystem fs = fileSystem.getFileSystem();
    Path appPath = new Path(metainfoPath);

    InputStream metainfoJsonStream = SliderUtils.getApplicationResourceInputStream(
        fs, appPath, "metainfo.json");
    if (metainfoJsonStream == null) {
      InputStream metainfoXMLStream = SliderUtils.getApplicationResourceInputStream(
          fs, appPath, "metainfo.xml");
      if (metainfoXMLStream != null) {
        metainfo = parseMetainfo(metainfoXMLStream, metainfoForAddon, "xml");
      }
    } else {
      metainfo = parseMetainfo(metainfoJsonStream, metainfoForAddon, "json");
    }

    if (metainfo == null) {
      log.error("metainfo is unavailable at {}.", metainfoPath);
      throw new FileNotFoundException("metainfo.xml/json is required in app package. " +
                                      appPath);
    }
    return metainfo;
  }

  private static Metainfo parseMetainfo(InputStream stream,
      boolean metainfoForAddon, String type) throws IOException {
    AbstractMetainfoParser metainfoParser = null;
    if (metainfoForAddon) {
      metainfoParser = new AddonPackageMetainfoParser();
    } else {
      metainfoParser = new MetainfoParser();
    }
    if (type.equals("xml")) {
      return metainfoParser.fromXmlStream(stream);
    } else if (type.equals("json")) {
      return metainfoParser.fromJsonStream(stream);
    }
    return null;
  }

  static DefaultConfig getDefaultConfig(SliderFileSystem fileSystem,
                                        String appDef, String configFileName)
      throws IOException {
    // this is the path inside the zip file
    String fileToRead = "configuration/" + configFileName;
    log.info("Reading default config file {} at {}", fileToRead, appDef);
    InputStream configStream = SliderUtils.getApplicationResourceInputStream(
        fileSystem.getFileSystem(), new Path(appDef), fileToRead);
    if (configStream == null) {
      log.error("{} is unavailable at {}.", fileToRead, appDef);
      throw new IOException("Expected config file " + fileToRead + " is not available.");
    }

    return new DefaultConfigParser().parse(configStream);
  }

  static String getMetainfoComponentName(String roleGroup,
      ConfTreeOperations appConf) throws BadConfigException {
    String prefix = appConf.getComponentOpt(roleGroup, ROLE_PREFIX, null);
    if (prefix == null) {
      return roleGroup;
    }
    if (!roleGroup.startsWith(prefix)) {
      throw new BadConfigException("Component " + roleGroup + " doesn't start" +
          " with prefix " + prefix);
    }
    return roleGroup.substring(prefix.length());
  }
}
