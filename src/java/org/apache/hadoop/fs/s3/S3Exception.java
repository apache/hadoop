package org.apache.hadoop.fs.s3;

/**
 * Thrown if there is a problem communicating with Amazon S3.
 */
public class S3Exception extends RuntimeException {

  public S3Exception(Throwable t) {
    super(t);
  }

}
