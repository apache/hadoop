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
import org.apache.hadoop.ozone.web.client.OzoneRestClientException;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.Shell;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Executes update volume calls.
 */
public class UpdateVolumeHandler extends Handler {
  private String rootName;
  private String ownerName;
  private String volumeName;
  private String quota;

  /**
   * Executes update volume calls.
   *
   * @param cmd - CommandLine
   * @throws IOException
   * @throws OzoneException
   * @throws URISyntaxException
   */
  @Override
  protected void execute(CommandLine cmd)
      throws IOException, OzoneException, URISyntaxException {
    if (!cmd.hasOption(Shell.UPDATE_VOLUME)) {
      throw new OzoneRestClientException(
          "Incorrect call : updateVolume is missing");
    }

    String ozoneURIString = cmd.getOptionValue(Shell.UPDATE_VOLUME);
    URI ozoneURI = verifyURI(ozoneURIString);
    if (ozoneURI.getPath().isEmpty()) {
      throw new OzoneRestClientException(
          "Volume name is required to update a volume");
    }

    // we need to skip the slash in the URI path
    volumeName = ozoneURI.getPath().substring(1);

    if (cmd.hasOption(Shell.RUNAS)) {
      rootName = "hdfs";
    } else {
      rootName = System.getProperty("user.name");
    }

    if (cmd.hasOption(Shell.QUOTA)) {
      quota = cmd.getOptionValue(Shell.QUOTA);
    }

    if (cmd.hasOption(Shell.USER)) {
      ownerName = cmd.getOptionValue(Shell.USER);
    }

    client.setEndPointURI(ozoneURI);
    client.setUserAuth(rootName);

    if (quota != null && !quota.isEmpty()) {
      client.setVolumeQuota(volumeName, quota);
    }

    if (ownerName != null && !ownerName.isEmpty()) {
      client.setVolumeOwner(volumeName, ownerName);
    }

  }
}
