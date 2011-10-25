/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

/**
 * Get, remove and modify table descriptors.
 * Used by servers to host descriptors.
 */
public interface TableDescriptors {
  /**
   * @param tablename
   * @return HTableDescriptor for tablename
   * @throws TableExistsException
   * @throws FileNotFoundException
   * @throws IOException
   */
  public HTableDescriptor get(final String tablename)
  throws TableExistsException, FileNotFoundException, IOException;

  /**
   * @param tablename
   * @return HTableDescriptor for tablename
   * @throws TableExistsException
   * @throws FileNotFoundException
   * @throws IOException
   */
  public HTableDescriptor get(final byte[] tablename)
  throws TableExistsException, FileNotFoundException, IOException;

  /**
   * Get Map of all HTableDescriptors. Populates the descriptor cache as a
   * side effect.
   * @return Map of all descriptors.
   * @throws IOException
   */
  public Map<String, HTableDescriptor> getAll()
  throws IOException;

  /**
   * Add or update descriptor
   * @param htd Descriptor to set into TableDescriptors
   * @throws IOException
   */
  public void add(final HTableDescriptor htd)
  throws IOException;

  /**
   * @param tablename
   * @return Instance of table descriptor or null if none found.
   * @throws IOException
   */
  public HTableDescriptor remove(final String tablename)
  throws IOException;
}
