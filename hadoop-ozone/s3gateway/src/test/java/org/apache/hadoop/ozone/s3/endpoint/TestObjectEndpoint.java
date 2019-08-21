package org.apache.hadoop.ozone.s3.endpoint;

import org.apache.hadoop.ozone.s3.exception.OS3Exception;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test static utility methods of the ObjectEndpoint.
 */
public class TestObjectEndpoint {

  @Test
  public void parseSourceHeader() throws OS3Exception {
    Pair<String, String> bucketKey =
        ObjectEndpoint.parseSourceHeader("bucket1/key1");

    Assert.assertEquals("bucket1", bucketKey.getLeft());

    Assert.assertEquals("key1", bucketKey.getRight());
  }

  @Test
  public void parseSourceHeaderWithPrefix() throws OS3Exception {
    Pair<String, String> bucketKey =
        ObjectEndpoint.parseSourceHeader("/bucket1/key1");

    Assert.assertEquals("bucket1", bucketKey.getLeft());

    Assert.assertEquals("key1", bucketKey.getRight());
  }

}