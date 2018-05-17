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
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.rest.response.BucketInfo;
import org.apache.hadoop.ozone.client.OzoneClientException;
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
 * Executes List Bucket.
 */
public class ListBucketHandler extends Handler {
  private String volumeName;

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
      throw new OzoneClientException(
          "Incorrect call : listBucket is missing");
    }

    String ozoneURIString = cmd.getOptionValue(Shell.LIST_BUCKET);
    URI ozoneURI = verifyURI(ozoneURIString);
    Path path = Paths.get(ozoneURI.getPath());
    if (path.getNameCount() < 1) {
      throw new OzoneClientException("volume is required in listBucket");
    }

    volumeName = path.getName(0).toString();

    if (cmd.hasOption(Shell.VERBOSE)) {
      System.out.printf("Volume Name : %s%n", volumeName);
    }

    int maxBuckets = Integer.MAX_VALUE;
    if (cmd.hasOption(Shell.LIST_LENGTH)) {
      String length = cmd.getOptionValue(Shell.LIST_LENGTH);
      OzoneUtils.verifyMaxKeyLength(length);
      maxBuckets = Integer.parseInt(length);
    }

    String startBucket = null;
    if (cmd.hasOption(Shell.START)) {
      startBucket = cmd.getOptionValue(Shell.START);
    }

    String prefix = null;
    if (cmd.hasOption(Shell.PREFIX)) {
      prefix = cmd.getOptionValue(Shell.PREFIX);
    }

    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    Iterator<OzoneBucket> bucketIterator = vol.listBuckets(prefix, startBucket);
    List<BucketInfo> bucketList = new ArrayList<>();
    while (maxBuckets > 0 && bucketIterator.hasNext()) {
      BucketInfo bucketInfo = OzoneClientUtils.asBucketInfo(bucketIterator.next());
      bucketList.add(bucketInfo);
      maxBuckets -= 1;
    }

    if (cmd.hasOption(Shell.VERBOSE)) {
      System.out.printf("Found : %d buckets for volume : %s ",
          bucketList.size(), volumeName);
    }
    System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(
        JsonUtils.toJsonString(bucketList)));
  }
}

