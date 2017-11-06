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
import org.apache.hadoop.ozone.web.utils.JsonUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Allows users to add and remove acls and from a bucket.
 */
public class UpdateBucketHandler extends Handler {
  private String volumeName;
  private String bucketName;
  private String rootName;

  @Override
  protected void execute(CommandLine cmd)
      throws IOException, OzoneException, URISyntaxException {
    if (!cmd.hasOption(Shell.UPDATE_BUCKET)) {
      throw new OzoneRestClientException(
          "Incorrect call : updateBucket is missing");
    }

    String ozoneURIString = cmd.getOptionValue(Shell.UPDATE_BUCKET);
    URI ozoneURI = verifyURI(ozoneURIString);
    Path path = Paths.get(ozoneURI.getPath());

    if (path.getNameCount() < 2) {
      throw new OzoneRestClientException(
          "volume and bucket name required in update bucket");
    }

    volumeName = path.getName(0).toString();
    bucketName = path.getName(1).toString();

    if (cmd.hasOption(Shell.VERBOSE)) {
      System.out.printf("Volume Name : %s%n", volumeName);
      System.out.printf("Bucket Name : %s%n", bucketName);
    }

    if (cmd.hasOption(Shell.RUNAS)) {
      rootName = "hdfs";
    } else {
      rootName = System.getProperty("user.name");
    }

    client.setEndPointURI(ozoneURI);
    client.setUserAuth(rootName);

    OzoneVolume vol = client.getVolume(volumeName);
    if (cmd.hasOption(Shell.ADD_ACLS)) {
      String aclString = cmd.getOptionValue(Shell.ADD_ACLS);
      String[] aclArray = aclString.split(",");
      vol.addAcls(bucketName, aclArray);
    }

    if (cmd.hasOption(Shell.REMOVE_ACLS)) {
      String aclString = cmd.getOptionValue(Shell.REMOVE_ACLS);
      String[] aclArray = aclString.split(",");
      vol.removeAcls(bucketName, aclArray);
    }

    OzoneBucket bucket = vol.getBucket(bucketName);
    System.out.printf("%s%n", JsonUtils.toJsonStringWithDefaultPrettyPrinter(
        bucket.getBucketInfo().toJsonString()));
  }
}
