/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.service.api.records.KerberosPrincipal;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.RunJobParameters;
import org.apache.hadoop.yarn.submarine.common.fs.RemoteDirectoryManager;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.FileSystemOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * Simple factory that creates a {@link KerberosPrincipal}.
 */
public final class KerberosPrincipalFactory {
  private KerberosPrincipalFactory() {
    throw new UnsupportedOperationException("This class should not be " +
        "instantiated!");
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(KerberosPrincipalFactory.class);

  public static KerberosPrincipal create(FileSystemOperations fsOperations,
      RemoteDirectoryManager remoteDirectoryManager,
      RunJobParameters parameters) throws IOException {
    Objects.requireNonNull(fsOperations,
        "FileSystemOperations must not be null!");
    Objects.requireNonNull(remoteDirectoryManager,
        "RemoteDirectoryManager must not be null!");
    Objects.requireNonNull(parameters, "Parameters must not be null!");

    if (StringUtils.isNotBlank(parameters.getKeytab()) && StringUtils
        .isNotBlank(parameters.getPrincipal())) {
      String keytab = parameters.getKeytab();
      String principal = parameters.getPrincipal();
      if (parameters.isDistributeKeytab()) {
        return handleDistributedKeytab(fsOperations, remoteDirectoryManager,
            parameters, keytab, principal);
      } else {
        return handleNormalKeytab(keytab, principal);
      }
    }
    LOG.debug("Principal and keytab was null or empty, " +
        "returning null KerberosPrincipal!");
    return null;
  }

  private static KerberosPrincipal handleDistributedKeytab(
      FileSystemOperations fsOperations,
      RemoteDirectoryManager remoteDirectoryManager,
      RunJobParameters parameters, String keytab, String principal)
      throws IOException {
    Path stagingDir = remoteDirectoryManager
        .getJobStagingArea(parameters.getName(), true);
    Path remoteKeytabPath =
        fsOperations.uploadToRemoteFile(stagingDir, keytab);
    // Only the owner has read access
    fsOperations.setPermission(remoteKeytabPath,
        FsPermission.createImmutable((short)Integer.parseInt("400", 8)));
    return new KerberosPrincipal()
        .keytab(remoteKeytabPath.toString())
        .principalName(principal);
  }

  private static KerberosPrincipal handleNormalKeytab(String keytab,
      String principal) {
    if(!keytab.startsWith("file")) {
      keytab = "file://" + keytab;
    }
    return new KerberosPrincipal()
        .keytab(keytab)
        .principalName(principal);
  }
}
