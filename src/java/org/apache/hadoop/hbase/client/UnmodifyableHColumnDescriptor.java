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

  /**
   * @see org.apache.hadoop.hbase.HColumnDescriptor#setValue(byte[], byte[])
   */
  @Override
  public void setValue(byte[] key, byte[] value) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  /**
   * @see org.apache.hadoop.hbase.HColumnDescriptor#setValue(java.lang.String, java.lang.String)
   */
  @Override
  public void setValue(String key, String value) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  /**
   * @see org.apache.hadoop.hbase.HColumnDescriptor#setMaxVersions(int)
   */
  @Override
  public void setMaxVersions(int maxVersions) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  /**
   * @see org.apache.hadoop.hbase.HColumnDescriptor#setInMemory(boolean)
   */
  @Override
  public void setInMemory(boolean inMemory) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  /**
   * @see org.apache.hadoop.hbase.HColumnDescriptor#setBlockCacheEnabled(boolean)
   */
  @Override
  public void setBlockCacheEnabled(boolean blockCacheEnabled) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  /**
   * @see org.apache.hadoop.hbase.HColumnDescriptor#setMaxValueLength(int)
   */
  @Override
  public void setMaxValueLength(int maxLength) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  /**
   * @see org.apache.hadoop.hbase.HColumnDescriptor#setTimeToLive(int)
   */
  @Override
  public void setTimeToLive(int timeToLive) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  /**
   * @see org.apache.hadoop.hbase.HColumnDescriptor#setCompressionType(org.apache.hadoop.hbase.io.hfile.Compression.Algorithm)
   */
  @Override
  public void setCompressionType(Compression.Algorithm type) {
    throw new UnsupportedOperationException("HColumnDescriptor is read-only");
  }

  /**
   * @see org.apache.hadoop.hbase.HColumnDescriptor#setMapFileIndexInterval(int)
   */
  @Override
  public void setMapFileIndexInterval(int interval) {
    throw new UnsupportedOperationException("HTableDescriptor is read-only");
  }
}