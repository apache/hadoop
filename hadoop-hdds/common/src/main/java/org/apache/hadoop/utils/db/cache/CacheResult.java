package org.apache.hadoop.utils.db.cache;

import java.util.Objects;

/**
 * CacheResult which is returned as response for Key exist in cache or not.
 * @param <CACHEVALUE>
 */
public class CacheResult<CACHEVALUE extends CacheValue> {

  private CacheStatus cacheStatus;
  private CACHEVALUE cachevalue;

  public CacheResult(CacheStatus cacheStatus, CACHEVALUE cachevalue) {
    this.cacheStatus = cacheStatus;
    this.cachevalue = cachevalue;
  }

  public CacheStatus getCacheStatus() {
    return cacheStatus;
  }

  public CACHEVALUE getValue() {
    return cachevalue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CacheResult< ? > that = (CacheResult< ? >) o;
    return cacheStatus == that.cacheStatus &&
        Objects.equals(cachevalue, that.cachevalue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cacheStatus, cachevalue);
  }

  /**
   * Status which tells whether key exists in cache or not.
   */
  public enum CacheStatus {
    EXISTS, // When key exists in cache.

    NOT_EXIST, // We guarantee that it does not exist. This will be returned
    // when the key does not exist in cache, when cache clean up policy is
    // NEVER.
    MAY_EXIST  // This will be returned when the key does not exist in
    // cache, when cache clean up policy is MANUAL. So caller need to check
    // if it might exist in it's rocksdb table.
  }
}
