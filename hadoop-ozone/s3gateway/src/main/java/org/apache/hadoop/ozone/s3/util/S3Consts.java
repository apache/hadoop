package org.apache.hadoop.ozone.s3.util;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Set of constants used for S3 implementation.
 */
@InterfaceAudience.Private
public final class S3Consts {

  //Never Constructed
  private S3Consts() {

  }

  public static final String COPY_SOURCE_HEADER = "x-amz-copy-source";
  public static final String STORAGE_CLASS_HEADER = "x-amz-storage-class";

}
