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
import org.apache.hadoop.ozone.web.client.OzoneVolume;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.Shell;
import org.apache.hadoop.ozone.web.response.VolumeInfo;
import org.apache.hadoop.ozone.web.utils.JsonUtils;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Executes List Volume call.
 */
public class ListVolumeHandler extends Handler {
  private String rootName;
  private String userName;

  /**
   * Executes the Client Calls.
   *
   * @param cmd - CommandLine
   * @throws IOException
   * @throws OzoneException
   * @throws URISyntaxException
   */
  @Override
  protected void execute(CommandLine cmd)
      throws IOException, OzoneException, URISyntaxException {

    if (!cmd.hasOption(Shell.LIST_VOLUME)) {
      throw new OzoneRestClientException(
          "Incorrect call : listVolume is missing");
    }

    int maxKeys = 0;
    if (cmd.hasOption(Shell.LIST_LENGTH)) {
      String length = cmd.getOptionValue(Shell.LIST_LENGTH);
      OzoneUtils.verifyMaxKeyLength(length);

      maxKeys = Integer.parseInt(length);
    }

    String startVolume = null;
    if (cmd.hasOption(Shell.START)) {
      startVolume = cmd.getOptionValue(Shell.START);
    }

    String prefix = null;
    if (cmd.hasOption(Shell.PREFIX)) {
      prefix = cmd.getOptionValue(Shell.PREFIX);
    }

    String ozoneURIString = cmd.getOptionValue(Shell.LIST_VOLUME);
    URI ozoneURI = verifyURI(ozoneURIString);

    if (cmd.hasOption(Shell.RUNAS)) {
      rootName = "hdfs";
    }

    if (cmd.hasOption(Shell.USER)) {
      userName = cmd.getOptionValue(Shell.USER);
    } else {
      userName = System.getProperty("user.name");
    }

    client.setEndPointURI(ozoneURI);
    if (rootName != null) {
      client.setUserAuth(rootName);
    } else {
      client.setUserAuth(userName);
    }

    List<OzoneVolume> volumes = client.listVolumes(userName, prefix, maxKeys,
        startVolume);
    if (volumes != null) {
      if (cmd.hasOption(Shell.VERBOSE)) {
        System.out.printf("Found : %d volumes for user : %s %n", volumes.size(),
            userName);
      }

      List<VolumeInfo> jsonData = volumes.stream()
          .map(OzoneVolume::getVolumeInfo).collect(Collectors.toList());
      System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(
          JsonUtils.toJsonString(jsonData)));
    }
  }
}

