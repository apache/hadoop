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
import org.apache.hadoop.ozone.web.client.OzoneClientException;
import org.apache.hadoop.ozone.web.client.OzoneVolume;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.Shell;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

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
      throw new OzoneClientException(
          "Incorrect call : listVolume is missing");
    }

    String ozoneURIString = cmd.getOptionValue(Shell.LIST_VOLUME);
    URI ozoneURI = verifyURI(ozoneURIString);

    if (cmd.hasOption(Shell.RUNAS)) {
      rootName = "hdfs";
    }

    if (!cmd.hasOption(Shell.USER)) {
      throw new OzoneClientException(
          "User name is needed in listVolume call.");
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

    List<OzoneVolume> volumes = client.listVolumes(userName);
    if (volumes != null) {
      if (cmd.hasOption(Shell.VERBOSE)) {
        System.out.printf("Found : %d volumes for user : %s %n", volumes.size(),
            userName);
      }
      ObjectMapper mapper = new ObjectMapper();

      for (OzoneVolume vol : volumes) {
        Object json = mapper.readValue(vol.getJsonString(), Object.class);
        System.out.printf("%s%n", mapper.writerWithDefaultPrettyPrinter()
            .writeValueAsString(json));
      }

    }
  }
}

