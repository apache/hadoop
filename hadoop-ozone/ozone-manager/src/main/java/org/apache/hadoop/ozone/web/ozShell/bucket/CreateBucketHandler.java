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

import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.OzoneAddress;
import org.apache.hadoop.ozone.web.ozShell.Shell;
import org.apache.hadoop.ozone.web.utils.JsonUtils;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
/**
 * create bucket handler.
 */
@Command(name = "create",
    description = "creates a bucket in a given volume")
public class CreateBucketHandler extends Handler {

  @Parameters(arity = "1..1", description = Shell.OZONE_BUCKET_URI_DESCRIPTION)
  private String uri;

  @Option(names = {"--bucketkey", "-k"},
      description = "bucket encryption key name")
  private String bekName;

  /**
   * Executes create bucket.
   */
  @Override
  public Void call() throws Exception {

    OzoneAddress address = new OzoneAddress(uri);
    address.ensureBucketAddress();
    OzoneClient client = address.createClient(createOzoneConfiguration());

    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();

    BucketArgs.Builder bb = new BucketArgs.Builder()
        .setStorageType(StorageType.DEFAULT)
        .setVersioning(false);

    if (bekName != null) {
      if (!bekName.isEmpty()) {
        bb.setBucketEncryptionKey(bekName);
      } else {
        throw new IllegalArgumentException("Bucket encryption key name must " +
            "be specified to enable bucket encryption!");
      }
    }

    if (isVerbose()) {
      System.out.printf("Volume Name : %s%n", volumeName);
      System.out.printf("Bucket Name : %s%n", bucketName);
      if (bekName != null) {
        bb.setBucketEncryptionKey(bekName);
        System.out.printf("Bucket Encryption enabled with Key Name: %s%n",
            bekName);
      }
    }

    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    vol.createBucket(bucketName, bb.build());

    if (isVerbose()) {
      OzoneBucket bucket = vol.getBucket(bucketName);
      System.out.printf(JsonUtils.toJsonStringWithDefaultPrettyPrinter(
          JsonUtils.toJsonString(OzoneClientUtils.asBucketInfo(bucket))));
    }
    return null;
  }
}
