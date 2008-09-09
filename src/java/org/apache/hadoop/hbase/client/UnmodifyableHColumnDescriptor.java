package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HColumnDescriptor;

/**
 * Immutable HColumnDescriptor
 */
public class UnmodifyableHColumnDescriptor extends HColumnDescriptor {

  /**
   * @param desc
   */
  public UnmodifyableHColumnDescriptor (final HColumnDescriptor desc) {
    super(desc);
  }

  @Override
  @SuppressWarnings("unused")
  public void setValue(byte[] key, byte[] value) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  @Override
  @SuppressWarnings("unused")
  public void setValue(String key, String value) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  @Override
  @SuppressWarnings("unused")
  public void setMaxVersions(int maxVersions) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  @Override
  @SuppressWarnings("unused")
  public void setInMemory(boolean inMemory) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  @Override
  @SuppressWarnings("unused")
  public void setBlockCacheEnabled(boolean blockCacheEnabled) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  @Override
  @SuppressWarnings("unused")
  public void setMaxValueLength(int maxLength) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  @Override
  @SuppressWarnings("unused")
  public void setTimeToLive(int timeToLive) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  @Override
  @SuppressWarnings("unused")
  public void setCompressionType(CompressionType type) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  @Override
  @SuppressWarnings("unused")
  public void setMapFileIndexInterval(int interval) {
    throw new UnsupportedOperationException("HTableDescriptor is read-only");
  }
}
