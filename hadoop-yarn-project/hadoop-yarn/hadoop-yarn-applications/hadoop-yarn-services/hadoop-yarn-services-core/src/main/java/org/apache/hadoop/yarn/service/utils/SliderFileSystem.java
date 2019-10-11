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

package org.apache.hadoop.yarn.service.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.service.conf.YarnServiceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Extends Core Filesystem with operations to manipulate ClusterDescription
 * persistent state
 */
public class SliderFileSystem extends CoreFileSystem {

  Path appDir = null;

  public SliderFileSystem(FileSystem fileSystem,
      Configuration configuration) {
    super(fileSystem, configuration);
  }

  public SliderFileSystem(Configuration configuration) throws IOException {
    super(configuration);
  }

  public void setAppDir(Path appDir) {
    this.appDir = appDir;
  }

  public Path getAppDir() {
    return this.appDir;
  }

  /**
   * Returns the component directory path.
   *
   * @param serviceVersion service version
   * @param compName       component name
   * @return component directory
   */
  public Path getComponentDir(String serviceVersion, String compName) {
    return new Path(new Path(getAppDir(), "components"),
        serviceVersion + "/" + compName);
  }

  public Path getBasePath() {
    String tmpDir = configuration.get("hadoop.tmp.dir");
    String basePath = YarnServiceConstants.SERVICE_BASE_DIRECTORY
        + "/" + YarnServiceConstants.SERVICES_DIRECTORY;
    return new Path(tmpDir, basePath);
  }

  /**
   * Returns the component public resource directory path.
   *
   * @param serviceVersion service version
   * @param compName       component name
   * @return component public resource directory
   */
  public Path getComponentPublicResourceDir(String serviceVersion,
      String compName) {
    return new Path(new Path(getBasePath(), getAppDir().getName() + "/"
        + "components"), serviceVersion + "/" + compName);
  }

  /**
   * Deletes the component directory.
   *
   * @param serviceVersion
   * @param compName
   * @throws IOException
   */
  public void deleteComponentDir(String serviceVersion, String compName)
      throws IOException {
    Path path = getComponentDir(serviceVersion, compName);
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true);
      LOG.debug("deleted dir {}", path);
    }
    Path publicResourceDir = getComponentPublicResourceDir(serviceVersion,
        compName);
    if (fileSystem.exists(publicResourceDir)) {
      fileSystem.delete(publicResourceDir, true);
      LOG.debug("deleted public resource dir {}", publicResourceDir);
    }
  }

  /**
   * Deletes the components version directory.
   *
   * @param serviceVersion
   * @throws IOException
   */
  public void deleteComponentsVersionDirIfEmpty(String serviceVersion)
      throws IOException {
    Path path = new Path(new Path(getAppDir(), "components"), serviceVersion);
    if (fileSystem.exists(path) && fileSystem.listStatus(path).length == 0) {
      fileSystem.delete(path, true);
      LOG.info("deleted dir {}", path);
    }
    Path publicResourceDir = new Path(new Path(getBasePath(),
        getAppDir().getName() + "/" + "components"), serviceVersion);
    if (fileSystem.exists(publicResourceDir)
        && fileSystem.listStatus(publicResourceDir).length == 0) {
      fileSystem.delete(publicResourceDir, true);
      LOG.info("deleted public resource dir {}", publicResourceDir);
    }
  }


  private static final Logger LOG = LoggerFactory.getLogger(
      SliderFileSystem.class);
}
