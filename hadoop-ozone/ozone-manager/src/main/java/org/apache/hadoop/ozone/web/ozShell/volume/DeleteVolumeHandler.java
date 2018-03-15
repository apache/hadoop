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
 * Executes deleteVolume call for the shell.
 */
public class DeleteVolumeHandler extends Handler {

  private String volumeName;
  private String rootName;

  /**
   * Executes the delete volume call.
   *
   * @param cmd - CommandLine
   * @throws IOException
   * @throws OzoneException
   * @throws URISyntaxException
   */
  @Override
  protected void execute(CommandLine cmd)
      throws IOException, OzoneException, URISyntaxException {

    if (!cmd.hasOption(Shell.DELETE_VOLUME)) {
      throw new OzoneRestClientException(
          "Incorrect call : deleteVolume call is missing");
    }

    String ozoneURIString = cmd.getOptionValue(Shell.DELETE_VOLUME);
    URI ozoneURI = verifyURI(ozoneURIString);
    if (ozoneURI.getPath().isEmpty()) {
      throw new OzoneRestClientException(
          "Volume name is required to delete a volume");
    }

    // we need to skip the slash in the URI path
    volumeName = ozoneURI.getPath().substring(1);

    if (cmd.hasOption(Shell.VERBOSE)) {
      System.out.printf("Volume name : %s%n", volumeName);
    }

    if (cmd.hasOption(Shell.RUNAS)) {
      rootName = "hdfs";
    } else {
      rootName = System.getProperty("user.name");
    }

    client.setEndPointURI(ozoneURI);
    client.setUserAuth(rootName);
    client.deleteVolume(volumeName);

  }
}
