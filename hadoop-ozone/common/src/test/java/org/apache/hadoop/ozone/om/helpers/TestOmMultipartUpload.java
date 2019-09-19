/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.helpers;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test utilities inside OmMutipartUpload.
 */
public class TestOmMultipartUpload {

  @Test
  public void from() {
    String key1 =
        OmMultipartUpload.getDbKey("vol1", "bucket1", "dir1/key1", "uploadId");
    OmMultipartUpload info = OmMultipartUpload.from(key1);

    Assert.assertEquals("vol1", info.getVolumeName());
    Assert.assertEquals("bucket1", info.getBucketName());
    Assert.assertEquals("dir1/key1", info.getKeyName());
    Assert.assertEquals("uploadId", info.getUploadId());
  }
}