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

package org.apache.hadoop.ozone.web.ozShell.keys;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.ozone.web.client.OzoneRestClientException;
import org.apache.hadoop.ozone.web.client.OzoneKey;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.Shell;
import org.apache.hadoop.ozone.web.response.KeyInfo;
import org.apache.hadoop.ozone.web.utils.JsonUtils;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Executes List Keys.
 */
public class ListKeyHandler extends Handler {
  private String userName;
  private String volumeName;
  private String bucketName;

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

    if (!cmd.hasOption(Shell.LIST_KEY)) {
      throw new OzoneRestClientException(
          "Incorrect call : listKey is missing");
    }

    String length = null;
    if (cmd.hasOption(Shell.LIST_LENGTH)) {
      length = cmd.getOptionValue(Shell.LIST_LENGTH);
      OzoneUtils.verifyMaxKeyLength(length);
    }

    String startKey = null;
    if (cmd.hasOption(Shell.START)) {
      startKey = cmd.getOptionValue(Shell.START);
    }

    String prefix = null;
    if (cmd.hasOption(Shell.PREFIX)) {
      prefix = cmd.getOptionValue(Shell.PREFIX);
    }

    String ozoneURIString = cmd.getOptionValue(Shell.LIST_KEY);
    URI ozoneURI = verifyURI(ozoneURIString);
    Path path = Paths.get(ozoneURI.getPath());
    if (path.getNameCount() < 2) {
      throw new OzoneRestClientException(
          "volume/bucket is required in listKey");
    }

    volumeName = path.getName(0).toString();
    bucketName = path.getName(1).toString();


    if (cmd.hasOption(Shell.VERBOSE)) {
      System.out.printf("Volume Name : %s%n", volumeName);
      System.out.printf("bucket Name : %s%n", bucketName);
    }

    if (cmd.hasOption(Shell.USER)) {
      userName = cmd.getOptionValue(Shell.USER);
    } else {
      userName = System.getProperty("user.name");
    }


    client.setEndPointURI(ozoneURI);
    client.setUserAuth(userName);

    List<OzoneKey> keys = client.listKeys(volumeName, bucketName, length,
        startKey, prefix);

    List<KeyInfo> jsonData = keys.stream()
        .map(OzoneKey::getObjectInfo).collect(Collectors.toList());
    System.out.printf(JsonUtils.toJsonStringWithDefaultPrettyPrinter(
        JsonUtils.toJsonString(jsonData)));
  }

}
