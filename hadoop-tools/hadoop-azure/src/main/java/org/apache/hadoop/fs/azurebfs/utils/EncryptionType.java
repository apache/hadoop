package org.apache.hadoop.fs.azurebfs.utils;

/**
 * Enum EncryptionType to represent the level of encryption applied:
 * GLOBAL_KEY for encrypting all files with the same client-provided key
 * ENCRYPTION_CONTEXT uses client-provided implementation to generate keys
 * NONE encryption handled entirely at server, no client
 */
public enum EncryptionType {
  GLOBAL_KEY,  // encrypt all files with the same client-provided key
  ENCRYPTION_CONTEXT,  // uses client-provided implementation to generate keys
  NONE // encryption handled entirely at server
}
