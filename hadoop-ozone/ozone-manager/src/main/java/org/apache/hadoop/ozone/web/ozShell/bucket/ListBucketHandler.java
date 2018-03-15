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

package org.apache.hadoop.ozone.web.ozShell.bucket;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.ozone.web.client.OzoneBucket;
import org.apache.hadoop.ozone.web.client.OzoneRestClientException;
import org.apache.hadoop.ozone.web.client.OzoneVolume;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.Shell;
import org.apache.hadoop.ozone.web.response.BucketInfo;
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
 * Executes List Bucket.
 */
public class ListBucketHandler extends Handler {
  private String volumeName;
  private String rootName;

  /**
   * Executes the Client Calls.
   *
   * @param cmd - CommandLine
   *
   * @throws IOException
   * @throws OzoneException
   * @throws URISyntaxException
   */
  @Override
  protected void execute(CommandLine cmd)
      throws IOException, OzoneException, URISyntaxException {
    if (!cmd.hasOption(Shell.LIST_BUCKET)) {
      throw new OzoneRestClientException(
          "Incorrect call : listBucket is missing");
    }

    String ozoneURIString = cmd.getOptionValue(Shell.LIST_BUCKET);
    URI ozoneURI = verifyURI(ozoneURIString);
    Path path = Paths.get(ozoneURI.getPath());
    if (path.getNameCount() < 1) {
      throw new OzoneRestClientException("volume is required in listBucket");
    }

    volumeName = path.getName(0).toString();


    if (cmd.hasOption(Shell.VERBOSE)) {
      System.out.printf("Volume Name : %s%n", volumeName);
    }

    if (cmd.hasOption(Shell.RUNAS)) {
      rootName = "hdfs";
    } else {
      rootName = System.getProperty("user.name");
    }


    client.setEndPointURI(ozoneURI);
    client.setUserAuth(rootName);

    String length = null;
    if (cmd.hasOption(Shell.LIST_LENGTH)) {
      length = cmd.getOptionValue(Shell.LIST_LENGTH);
      OzoneUtils.verifyMaxKeyLength(length);
    }

    String startBucket = null;
    if (cmd.hasOption(Shell.START)) {
      startBucket = cmd.getOptionValue(Shell.START);
    }

    String prefix = null;
    if (cmd.hasOption(Shell.PREFIX)) {
      prefix = cmd.getOptionValue(Shell.PREFIX);
    }

    OzoneVolume vol = client.getVolume(volumeName);
    List<OzoneBucket> bucketList = vol.listBuckets(length, startBucket, prefix);

    List<BucketInfo> jsonData = bucketList.stream()
        .map(OzoneBucket::getBucketInfo).collect(Collectors.toList());
    System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(
        JsonUtils.toJsonString(jsonData)));
  }
}

