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
package org.apache.hadoop.yarn.service.provider.tarball;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.service.api.records.Artifact;
import org.apache.hadoop.yarn.service.api.records.ConfigFile;
import org.apache.hadoop.yarn.service.conf.YarnServiceConstants;
import org.apache.hadoop.yarn.service.provider.AbstractClientProvider;
import org.apache.hadoop.yarn.service.exceptions.RestApiErrorMessages;

import java.io.IOException;
import java.nio.file.Paths;

public class TarballClientProvider extends AbstractClientProvider
    implements YarnServiceConstants {

  public TarballClientProvider() {
  }

  @Override
  public void validateArtifact(Artifact artifact, String compName,
      FileSystem fs) throws IOException {
    if (artifact == null) {
      throw new IllegalArgumentException(String.format(
          RestApiErrorMessages.ERROR_ARTIFACT_FOR_COMP_INVALID, compName));
    }
    if (StringUtils.isEmpty(artifact.getId())) {
      throw new IllegalArgumentException(String.format(
          RestApiErrorMessages.ERROR_ARTIFACT_ID_FOR_COMP_INVALID, compName));
    }
    Path p = new Path(artifact.getId());
    if (!fs.exists(p)) {
      throw new IllegalArgumentException(String.format(
          RestApiErrorMessages.ERROR_ARTIFACT_PATH_FOR_COMP_INVALID, compName,
          Artifact.TypeEnum.TARBALL.name(), artifact.getId()));
    }
  }

  @Override
  protected void validateConfigFile(ConfigFile configFile, String compName,
      FileSystem fileSystem) throws IOException {
    // validate dest_file is not absolute
    if (Paths.get(configFile.getDestFile()).isAbsolute()) {
      throw new IllegalArgumentException(String.format(
          RestApiErrorMessages.ERROR_CONFIGFILE_DEST_FILE_FOR_COMP_NOT_ABSOLUTE,
          compName, Artifact.TypeEnum.TARBALL.name(),
          configFile.getDestFile()));
    }
  }
}
