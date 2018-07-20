/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.web.ozShell.volume;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.Shell;
import org.apache.hadoop.ozone.web.utils.JsonUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Executes the create volume call for the shell.
 */
public class CreateVolumeHandler extends Handler {

  private String rootName;
  private String userName;
  private String volumeName;
  private String quota;

  /**
   * Executes the Create Volume.
   *
   * @param cmd - CommandLine
   * @throws IOException
   * @throws OzoneException
   * @throws URISyntaxException
   */
  @Override
  protected void execute(CommandLine cmd)
      throws IOException, OzoneException, URISyntaxException {
    if (!cmd.hasOption(Shell.CREATE_VOLUME)) {
      throw new OzoneClientException(
          "Incorrect call : createVolume is missing");
    }

    String ozoneURIString = cmd.getOptionValue(Shell.CREATE_VOLUME);
    URI ozoneURI = verifyURI(ozoneURIString);

    // we need to skip the slash in the URI path
    // getPath returns /volumeName needs to remove the initial slash.
    volumeName = ozoneURI.getPath().replaceAll("^/+", "");
    if (volumeName.isEmpty()) {
      throw new OzoneClientException(
          "Volume name is required to create a volume");
    }

    if (cmd.hasOption(Shell.VERBOSE)) {
      System.out.printf("Volume name : %s%n", volumeName);
    }
    if (cmd.hasOption(Shell.RUNAS)) {
      rootName = "hdfs";
    } else {
      rootName = System.getProperty("user.name");
    }

    if (!cmd.hasOption(Shell.USER)) {
      throw new OzoneClientException(
          "User name is needed in createVolume call.");
    }

    if (cmd.hasOption(Shell.QUOTA)) {
      quota = cmd.getOptionValue(Shell.QUOTA);
    }

    userName = cmd.getOptionValue(Shell.USER);

    VolumeArgs.Builder volumeArgsBuilder = VolumeArgs.newBuilder()
        .setAdmin(rootName)
        .setOwner(userName);
    if (quota != null) {
      volumeArgsBuilder.setQuota(quota);
    }
    client.getObjectStore().createVolume(volumeName, volumeArgsBuilder.build());

    if (cmd.hasOption(Shell.VERBOSE)) {
      OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
      System.out.printf("%s%n", JsonUtils.toJsonStringWithDefaultPrettyPrinter(
          JsonUtils.toJsonString(OzoneClientUtils.asVolumeInfo(vol))));
    }
  }
}

