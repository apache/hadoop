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

import java.util.Iterator;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.ObjectPrinter;
import org.apache.hadoop.ozone.web.ozShell.OzoneAddress;
import org.apache.hadoop.ozone.web.ozShell.Shell;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Executes List Keys.
 */
@Command(name = "list",
    aliases = "ls",
    description = "list all keys in a given bucket")
public class ListKeyHandler extends Handler {

  @Parameters(arity = "1..1", description = Shell.OZONE_BUCKET_URI_DESCRIPTION)
  private String uri;

  @Option(names = {"--length", "-l"},
      description = "Limit of the max results",
      defaultValue = "100")
  private int maxKeys;

  @Option(names = {"--start", "-s"},
      description = "The first key to start the listing")
  private String startKey;

  @Option(names = {"--prefix", "-p"},
      description = "Prefix to filter the key")
  private String prefix;

  /**
   * Executes the Client Calls.
   */
  @Override
  public Void call() throws Exception {

    OzoneAddress address = new OzoneAddress(uri);
    address.ensureBucketAddress();
    OzoneClient client = address.createClient(createOzoneConfiguration());

    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();

    if (maxKeys < 1) {
      throw new IllegalArgumentException(
          "the length should be a positive number");
    }

    if (isVerbose()) {
      System.out.printf("Volume Name : %s%n", volumeName);
      System.out.printf("bucket Name : %s%n", bucketName);
    }

    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = vol.getBucket(bucketName);
    Iterator<? extends OzoneKey> keyIterator = bucket.listKeys(prefix,
        startKey);

    int maxKeyLimit = maxKeys;

    int counter = 0;
    while (maxKeys > 0 && keyIterator.hasNext()) {
      OzoneKey ozoneKey = keyIterator.next();
      ObjectPrinter.printObjectAsJson(ozoneKey);
      maxKeys -= 1;
      counter++;
    }

    // More keys were returned notify about max length
    if (keyIterator.hasNext()) {
      System.out.println("Listing first " + maxKeyLimit + " entries of the " +
          "result. Use --length (-l) to override max returned keys.");
    } else if (isVerbose()) {
      System.out.printf("Found : %d keys for bucket %s in volume : %s ",
          counter, bucketName, volumeName);
    }

    return null;
  }

}
