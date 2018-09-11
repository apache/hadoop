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

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.Shell;
import org.apache.hadoop.ozone.web.utils.JsonUtils;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Allows users to add and remove acls and from a bucket.
 */
@Command(name = "update",
    description = "allows changing bucket attributes")
public class UpdateBucketHandler extends Handler {

  @Parameters(arity = "1..1", description = Shell.OZONE_BUCKET_URI_DESCRIPTION)
  private String uri;

  @Option(names = {"--addAcl"},
      description = "Comma separated list of acl rules to add (eg. " +
          "user:bilbo:rw)")
  private String addAcl;

  @Option(names = {"--removeAcl"},
      description = "Comma separated list of acl rules to remove (eg. "
          + "user:bilbo:rw)")
  private String removeAcl;

  @Override
  public Void call() throws Exception {

    URI ozoneURI = verifyURI(uri);
    Path path = Paths.get(ozoneURI.getPath());

    if (path.getNameCount() < 2) {
      throw new OzoneClientException(
          "volume and bucket name required in update bucket");
    }

    String volumeName = path.getName(0).toString();
    String bucketName = path.getName(1).toString();

    if (isVerbose()) {
      System.out.printf("Volume Name : %s%n", volumeName);
      System.out.printf("Bucket Name : %s%n", bucketName);
    }

    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = vol.getBucket(bucketName);
    if (addAcl != null) {
      String[] aclArray = addAcl.split(",");
      List<OzoneAcl> aclList =
          Arrays.stream(aclArray).map(acl -> OzoneAcl.parseAcl(acl))
              .collect(Collectors.toList());
      bucket.addAcls(aclList);
    }

    if (removeAcl != null) {
      String[] aclArray = removeAcl.split(",");
      List<OzoneAcl> aclList =
          Arrays.stream(aclArray).map(acl -> OzoneAcl.parseAcl(acl))
              .collect(Collectors.toList());
      bucket.removeAcls(aclList);
    }

    System.out.printf("%s%n", JsonUtils.toJsonStringWithDefaultPrettyPrinter(
        JsonUtils.toJsonString(OzoneClientUtils.asBucketInfo(bucket))));
    return null;
  }
}
