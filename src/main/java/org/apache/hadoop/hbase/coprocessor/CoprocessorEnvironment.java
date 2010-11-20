/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

/**
 * Coprocessor environment state.
 */
public interface CoprocessorEnvironment {

  /** @return the Coprocessor interface version */
  public int getVersion();

  /** @return the HBase version as a string (e.g. "0.21.0") */
  public String getHBaseVersion();

  /** @return the region associated with this coprocessor */
  public HRegion getRegion();

  /** @return reference to the region server services */
  public RegionServerServices getRegionServerServices();

  /**
   * @return an interface for accessing the given table
   * @throws IOException
   */
  public HTableInterface getTable(byte[] tableName) throws IOException;

  // environment variables

  /**
   * Get an environment variable
   * @param key the key
   * @return the object corresponding to the environment variable, if set
   */
  public Object get(Object key);

  /**
   * Set an environment variable
   * @param key the key
   * @param value the value
   */
  public void put(Object key, Object value);

  /**
   * Remove an environment variable
   * @param key the key
   * @return the object corresponding to the environment variable, if set
   */
  public Object remove(Object key);

}
