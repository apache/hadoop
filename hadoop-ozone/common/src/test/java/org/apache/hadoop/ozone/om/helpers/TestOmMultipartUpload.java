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