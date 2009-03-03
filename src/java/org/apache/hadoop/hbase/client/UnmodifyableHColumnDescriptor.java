package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression;

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
  public void setValue(byte[] key, byte[] value) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  @Override
  public void setValue(String key, String value) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  @Override
  public void setMaxVersions(int maxVersions) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  @Override
  public void setInMemory(boolean inMemory) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  @Override
  public void setBlockCacheEnabled(boolean blockCacheEnabled) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  @Override
  public void setMaxValueLength(int maxLength) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  @Override
  public void setTimeToLive(int timeToLive) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  @Override
  public void setCompressionType(Compression.Algorithm type) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  @Override
  public void setMapFileIndexInterval(int interval) {
    throw new UnsupportedOperationException("HTableDescriptor is read-only");
  }
}