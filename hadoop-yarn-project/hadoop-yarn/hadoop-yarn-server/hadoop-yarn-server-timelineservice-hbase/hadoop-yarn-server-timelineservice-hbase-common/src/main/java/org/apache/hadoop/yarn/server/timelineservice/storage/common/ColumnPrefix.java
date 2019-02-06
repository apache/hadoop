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
package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;

/**
 * Used to represent a partially qualified column, where the actual column name
 * will be composed of a prefix and the remainder of the column qualifier. The
 * prefix can be null, in which case the column qualifier will be completely
 * determined when the values are stored.
 */
public interface ColumnPrefix<T extends BaseTable<T>> {

  /**
   * @param qualifierPrefix Column qualifier or prefix of qualifier.
   * @return a byte array encoding column prefix and qualifier/prefix passed.
   */
  byte[] getColumnPrefixBytes(String qualifierPrefix);

  /**
   * @param qualifierPrefix Column qualifier or prefix of qualifier.
   * @return a byte array encoding column prefix and qualifier/prefix passed.
   */
  byte[] getColumnPrefixBytes(byte[] qualifierPrefix);

  /**
   * Get the column prefix in bytes.
   * @return column prefix in bytes
   */
  byte[] getColumnPrefixInBytes();

  /**
   * Returns column family name(as bytes) associated with this column prefix.
   * @return a byte array encoding column family for this prefix.
   */
  byte[] getColumnFamilyBytes();

  /**
   * Returns value converter implementation associated with this column prefix.
   * @return a {@link ValueConverter} implementation.
   */
  ValueConverter getValueConverter();

  /**
   * Return attributed combined with aggregations, if any.
   * @return an array of Attributes
   */
  Attribute[] getCombinedAttrsWithAggr(Attribute... attributes);

  /**
   * Return true if the cell timestamp needs to be supplemented.
   * @return true if the cell timestamp needs to be supplemented
   */
  boolean supplementCellTimeStamp();
}