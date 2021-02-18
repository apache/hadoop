package org.apache.hadoop.fs.s3a;

import com.amazonaws.services.s3.model.EncryptionMaterialsProvider;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * An interface to provide custom implementation for
 * EncryptionMaterialsProvider using AWS CSE - CUSTOM method in S3Client.
 */
@VisibleForTesting
public interface S3ACSEMaterialProviderConfig {
  EncryptionMaterialsProvider buildEncryptionProvider();
}
