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

package org.apache.hadoop.yarn.service.provider;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.slider.api.resource.Artifact;
import org.apache.slider.api.resource.ConfigFile;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.SliderException;
import org.codehaus.jettison.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class AbstractClientProvider {

  public AbstractClientProvider() {
  }

  /**
   * Generates a fixed format of application tags given one or more of
   * application name, version and description. This allows subsequent query for
   * an application with a name only, version only or description only or any
   * combination of those as filters.
   *
   * @param appName name of the application
   * @param appVersion version of the application
   * @param appDescription brief description of the application
   * @return
   */
  public static final Set<String> createApplicationTags(String appName,
      String appVersion, String appDescription) {
    Set<String> tags = new HashSet<>();
    tags.add(SliderUtils.createNameTag(appName));
    if (appVersion != null) {
      tags.add(SliderUtils.createVersionTag(appVersion));
    }
    if (appDescription != null) {
      tags.add(SliderUtils.createDescriptionTag(appDescription));
    }
    return tags;
  }

  /**
   * Validate the artifact.
   * @param artifact
   */
  public abstract void validateArtifact(Artifact artifact, FileSystem
      fileSystem) throws IOException;

  protected abstract void validateConfigFile(ConfigFile configFile, FileSystem
      fileSystem) throws IOException;

  /**
   * Validate the config files.
   * @param configFiles config file list
   * @param fs file system
   */
  public void validateConfigFiles(List<ConfigFile> configFiles,
      FileSystem fs) throws IOException {
    Set<String> destFileSet = new HashSet<>();

    for (ConfigFile file : configFiles) {
      if (file.getType() == null) {
        throw new IllegalArgumentException("File type is empty");
      }

      if (file.getType().equals(ConfigFile.TypeEnum.TEMPLATE) && StringUtils
          .isEmpty(file.getSrcFile())) {
        throw new IllegalArgumentException(
            "Src_file is empty for " + ConfigFile.TypeEnum.TEMPLATE);

      }
      if (!StringUtils.isEmpty(file.getSrcFile())) {
        Path p = new Path(file.getSrcFile());
        if (!fs.exists(p)) {
          throw new IllegalArgumentException(
              "Src_file does not exist for config file: " + file
                  .getSrcFile());
        }
      }

      if (StringUtils.isEmpty(file.getDestFile())) {
        throw new IllegalArgumentException("Dest_file is empty.");
      }

      if (destFileSet.contains(file.getDestFile())) {
        throw new IllegalArgumentException(
            "Duplicated ConfigFile exists: " + file.getDestFile());
      }
      destFileSet.add(file.getDestFile());

      java.nio.file.Path destPath = Paths.get(file.getDestFile());
      if (!destPath.isAbsolute() && destPath.getNameCount() > 1) {
        throw new IllegalArgumentException("Non-absolute dest_file has more " +
            "than one path element");
      }

      // provider-specific validation
      validateConfigFile(file, fs);
    }
  }
}
