/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.web.ozShell.keys;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.OzoneAddress;
import org.apache.hadoop.ozone.web.ozShell.Shell;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * Renames an existing key.
 */
@Command(name = "rename",
    description = "renames an existing key")
public class RenameKeyHandler extends Handler {

  @Parameters(index = "0", arity = "1..1",
      description = Shell.OZONE_BUCKET_URI_DESCRIPTION)
  private String uri;

  @Parameters(index = "1", arity = "1..1",
      description = "The existing key to be renamed")
  private String fromKey;

  @Parameters(index = "2", arity = "1..1",
      description = "The new desired name of the key")
  private String toKey;

  @Override
  public Void call() throws Exception {
    OzoneAddress address = new OzoneAddress(uri);
    address.ensureBucketAddress();
    OzoneClient client = address.createClient(createOzoneConfiguration());

    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();

    if (isVerbose()) {
      System.out.printf("Volume Name : %s%n", volumeName);
      System.out.printf("Bucket Name : %s%n", bucketName);
    }

    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = vol.getBucket(bucketName);
    bucket.renameKey(fromKey, toKey);

    if (isVerbose()) {
      System.out.printf("Renamed Key : %s to %s%n", fromKey, toKey);
    }

    return null;
  }
}
