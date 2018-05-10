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
import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.ozone.client.rest.response.KeyInfo;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.Shell;
import org.apache.hadoop.ozone.web.utils.JsonUtils;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Executes List Keys.
 */
public class ListKeyHandler extends Handler {
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
      throw new OzoneClientException(
          "Incorrect call : listKey is missing");
    }

    int maxKeys = Integer.MAX_VALUE;
    if (cmd.hasOption(Shell.LIST_LENGTH)) {
      String length = cmd.getOptionValue(Shell.LIST_LENGTH);
      OzoneUtils.verifyMaxKeyLength(length);
      maxKeys = Integer.parseInt(length);
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
      throw new OzoneClientException(
          "volume/bucket is required in listKey");
    }

    volumeName = path.getName(0).toString();
    bucketName = path.getName(1).toString();


    if (cmd.hasOption(Shell.VERBOSE)) {
      System.out.printf("Volume Name : %s%n", volumeName);
      System.out.printf("bucket Name : %s%n", bucketName);
    }

    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = vol.getBucket(bucketName);
    Iterator<OzoneKey> keyIterator = bucket.listKeys(prefix, startKey);
    List<KeyInfo> keyInfos = new ArrayList<>();

    while (maxKeys > 0 && keyIterator.hasNext()) {
      KeyInfo key = OzoneClientUtils.asKeyInfo(keyIterator.next());
      keyInfos.add(key);
      maxKeys -= 1;
    }

    if (cmd.hasOption(Shell.VERBOSE)) {
      System.out.printf("Found : %d keys for bucket %s in volume : %s ",
          keyInfos.size(), bucketName, volumeName);
    }
    System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(
        JsonUtils.toJsonString(keyInfos)));
  }

}
