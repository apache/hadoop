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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.Shell;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * S3Bucket mapping handler, which returns volume name and Ozone fs uri for
 * that bucket.
 */
@Command(name = "path",
    description = "Returns the ozone path for S3Bucket")
public class S3BucketMapping extends Handler {

  @Parameters(arity = "1..1", description = Shell
      .OZONE_S3BUCKET_URI_DESCRIPTION)
  private String uri;

  /**
   * Executes create bucket.
   */
  @Override
  public Void call() throws Exception {

    URI ozoneURI = verifyURI(uri);
    Path path = Paths.get(ozoneURI.getPath());
    int pathNameCount = path.getNameCount();
    String errorMessage;

    // When just uri is given as http://om:9874, we are getting pathCount
    // still as 1, as getPath() is returning empty string.
    // So for safer side check, whether it is an empty string
    if (pathNameCount == 1) {
      String s3Bucket = path.getName(0).toString();
      if (StringUtils.isBlank(s3Bucket)) {
        errorMessage = "S3Bucket name is required to get volume name and " +
            "Ozone fs Uri";
        throw new OzoneClientException(errorMessage);
      }
    }
    if (pathNameCount != 1) {
      if (pathNameCount < 1) {
        errorMessage = "S3Bucket name is required to get volume name and " +
            "Ozone fs Uri";
      } else {
        errorMessage = "Invalid S3Bucket name. Delimiters (/) not allowed in " +
            "S3Bucket name";
      }
      throw new OzoneClientException(errorMessage);
    }

    String s3Bucket = path.getName(0).toString();
    if (isVerbose()) {
      System.out.printf("S3Bucket Name : %s%n", s3Bucket);
    }

    String mapping = client.getObjectStore().getOzoneBucketMapping(s3Bucket);
    String volumeName = client.getObjectStore().getOzoneVolumeName(s3Bucket);

    if (isVerbose()) {
      System.out.printf("Mapping created for S3Bucket is : %s%n", mapping);
    }

    System.out.printf("Volume name for S3Bucket is : %s%n", volumeName);

    String ozoneFsUri = String.format("%s://%s.%s", OzoneConsts
        .OZONE_URI_SCHEME, s3Bucket, volumeName);

    System.out.printf("Ozone FileSystem Uri is : %s%n", ozoneFsUri);

    return null;
  }
}
