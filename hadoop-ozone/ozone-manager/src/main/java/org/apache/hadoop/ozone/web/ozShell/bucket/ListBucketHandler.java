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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.rest.response.BucketInfo;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.OzoneAddress;
import org.apache.hadoop.ozone.web.ozShell.Shell;
import org.apache.hadoop.ozone.web.utils.JsonUtils;

import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Visibility;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Executes List Bucket.
 */
@Command(name = "list",
    aliases = "ls",
    description = "lists the buckets in a volume.")
public class ListBucketHandler extends Handler {

  @Parameters(arity = "1..1", description = Shell.OZONE_VOLUME_URI_DESCRIPTION)
  private String uri;

  @Option(names = {"--length", "-l"},
      description = "Limit of the max results",
      defaultValue = "100",
      showDefaultValue = Visibility.ALWAYS)
  private int maxBuckets;

  @Option(names = {"--start", "-s"},
      description = "The first bucket to start the listing")
  private String startBucket;

  @Option(names = {"--prefix", "-p"},
      description = "Prefix to filter the buckets")
  private String prefix;
  /**
   * Executes the Client Calls.
   */
  @Override
  public Void call() throws Exception {

    OzoneAddress address = new OzoneAddress(uri);
    address.ensureVolumeAddress();
    OzoneClient client = address.createClient(createOzoneConfiguration());

    String volumeName = address.getVolumeName();
    if (maxBuckets < 1) {
      throw new IllegalArgumentException(
          "the length should be a positive number");
    }

    if (isVerbose()) {
      System.out.printf("Volume Name : %s%n", volumeName);
    }


    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    Iterator<? extends OzoneBucket> bucketIterator =
        vol.listBuckets(prefix, startBucket);
    List<BucketInfo> bucketList = new ArrayList<>();
    while (maxBuckets > 0 && bucketIterator.hasNext()) {
      BucketInfo bucketInfo =
          OzoneClientUtils.asBucketInfo(bucketIterator.next());
      bucketList.add(bucketInfo);
      maxBuckets -= 1;
    }

    if (isVerbose()) {
      System.out.printf("Found : %d buckets for volume : %s ",
          bucketList.size(), volumeName);
    }
    System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(
        JsonUtils.toJsonString(bucketList)));
    return null;
  }

}

