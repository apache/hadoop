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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om.response;

import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.s3.bucket.S3BucketCreateRequest;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketCreateResponse;
import org.apache.hadoop.ozone.om.response.s3.bucket.S3BucketCreateResponse;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.util.Time;

/**
 * Helper class to test OMClientResponse classes.
 */
public final class TestOMResponseUtils {

  // No one can instantiate, this is just utility class with all static methods.
  private TestOMResponseUtils() {
  }

  public static  OmBucketInfo createBucket(String volume, String bucket) {
    return OmBucketInfo.newBuilder().setVolumeName(volume).setBucketName(bucket)
        .setCreationTime(Time.now()).setIsVersionEnabled(true).addMetadata(
            "key1", "value1").build();

  }

  public static S3BucketCreateResponse createS3BucketResponse(String userName,
      String volumeName, String s3BucketName) {
    OzoneManagerProtocolProtos.OMResponse omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.CreateS3Bucket)
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setSuccess(true)
            .setCreateS3BucketResponse(
                OzoneManagerProtocolProtos.S3CreateBucketResponse
                    .getDefaultInstance())
            .build();

    OzoneManagerProtocolProtos.VolumeList volumeList =
        OzoneManagerProtocolProtos.VolumeList.newBuilder()
            .addVolumeNames(volumeName).build();

    OmVolumeArgs omVolumeArgs = OmVolumeArgs.newBuilder()
        .setOwnerName(userName).setAdminName(userName)
        .setVolume(volumeName).setCreationTime(Time.now()).build();

    OMVolumeCreateResponse omVolumeCreateResponse =
        new OMVolumeCreateResponse(omVolumeArgs, volumeList, omResponse);


    OmBucketInfo omBucketInfo = TestOMResponseUtils.createBucket(
        volumeName, s3BucketName);
    OMBucketCreateResponse omBucketCreateResponse =
        new OMBucketCreateResponse(omBucketInfo, omResponse);

    String s3Mapping = S3BucketCreateRequest.formatS3MappingName(volumeName,
        s3BucketName);
    return
        new S3BucketCreateResponse(omVolumeCreateResponse,
            omBucketCreateResponse, s3BucketName, s3Mapping, omResponse);
  }
}
