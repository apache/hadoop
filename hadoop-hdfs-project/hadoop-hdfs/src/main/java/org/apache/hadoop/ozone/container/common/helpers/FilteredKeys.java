/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.helpers;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.utils.LevelDBStore;
import org.iq80.leveldb.DBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * An utility class to get a list of filtered keys.
 */
public class FilteredKeys implements Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(FilteredKeys.class);

  private final DBIterator dbIterator;
  private final List<KeyFilter> filters;
  private int count = 1000;

  public FilteredKeys(LevelDBStore db, int count) {
    Preconditions.checkNotNull(db, "LeveDBStore cannot be null.");
    this.dbIterator = db.getIterator();
    dbIterator.seekToFirst();
    this.filters = new ArrayList<KeyFilter>();
    if(count > 0) {
      this.count = count;
    }
  }

  /**
   * Adds a key filter which filters keys by a certain criteria.
   * Valid key filter is an implementation of {@link KeyFilter} class.
   *
   * @param filter
   */
  public void addKeyFilter(KeyFilter filter) {
    filter.setDbIterator(dbIterator);
    filters.add(filter);
  }

  private boolean filter(String keyName) {
    if(filters != null && !filters.isEmpty()) {
      for(KeyFilter filter : filters) {
        if(!filter.check(keyName)) {
          return false;
        }
      }
    }
    return true;
  }

  public List<KeyData> getFilteredKeys() {
    List<KeyData> result = new ArrayList<KeyData>();
    while (dbIterator.hasNext() && result.size() < count) {
      Map.Entry<byte[], byte[]> entry = dbIterator.next();
      String keyName = KeyUtils.getKeyName(entry.getKey());
      if (filter(keyName)) {
        try {
          KeyData value = KeyUtils.getKeyData(entry.getValue());
          KeyData data = new KeyData(value.getContainerName(), keyName);
          result.add(data);
        } catch (IOException e) {
          LOG.warn("Ignoring adding an invalid entry", e);
        }
      }
    }
    return result;
  }

  @Override public void close() {
    if(dbIterator != null) {
      try {
        dbIterator.close();
      } catch (IOException e) {
        LOG.warn("Failed to close levelDB connection.", e);
      }
    }
  }

  /**
   * An abstract class for all key filters.
   */
  public static abstract class KeyFilter {

    private DBIterator dbIterator;

    /**
     * Returns if this filter is enabled.
     *
     * @return true if this filter is enabled, false otherwise.
     */
    abstract boolean isEnabled();

    /**
     * Filters the element by key name. Returns true if the key
     * with the given key name complies with the criteria defined
     * in this filter.
     *
     * @param keyName
     * @return true if filter passes and false otherwise.
     */
    abstract boolean filterKey(String keyName);

    /**
     * If this filter is enabled, returns true if the key with the
     * given key name complies with the criteria defined in this filter;
     * if this filter is disabled, always returns true.
     *
     * @param keyName
     * @return true if filter passes and false otherwise.
     */
    public boolean check(String keyName) {
      return isEnabled()? filterKey(keyName) : true;
    }

    /**
     * Set the {@link DBIterator} this filter used to iterate DB entries.
     *
     * @param dbIterator
     */
    protected void setDbIterator(DBIterator dbIterator) {
      this.dbIterator = dbIterator;
    }

    protected DBIterator getDbIterator() {
      return this.dbIterator;
    }
  }

  /**
   * Filters keys with a previous key name,
   * returns only the keys that whose position is behind the given key name.
   */
  public static class PreKeyFilter extends KeyFilter{

    private final String prevKey;
    private boolean preKeyFound = false;

    public PreKeyFilter(LevelDBStore db, String prevKey)  {
      Preconditions.checkNotNull(db, "LevelDB store cannot be null.");
      this.prevKey = prevKey;
    }

    @Override
    protected boolean isEnabled() {
      return !Strings.isNullOrEmpty(prevKey);
    }

    @Override
    protected boolean filterKey(String keyName) {
      if (preKeyFound) {
        return true;
      } else {
        if (getDbIterator().hasPrev()) {
          byte[] prevKeyBytes = getDbIterator().peekPrev().getKey();
          String prevKeyActual = KeyUtils.getKeyName(prevKeyBytes);
          if (prevKeyActual.equals(prevKey)) {
            preKeyFound = true;
          }
        }
        return false;
      }
    }
  }

  /**
   * Filters keys by a key name prefix.
   */
  public static class KeyPrefixFilter extends KeyFilter{

    private String prefix = null;

    public KeyPrefixFilter(String prefix)  {
      this.prefix = prefix;
    }

    @Override
    protected boolean isEnabled() {
      return !Strings.isNullOrEmpty(prefix);
    }

    @Override
    protected boolean filterKey(String keyName) {
      return keyName.startsWith(prefix) ? true : false;
    }
  }
}
