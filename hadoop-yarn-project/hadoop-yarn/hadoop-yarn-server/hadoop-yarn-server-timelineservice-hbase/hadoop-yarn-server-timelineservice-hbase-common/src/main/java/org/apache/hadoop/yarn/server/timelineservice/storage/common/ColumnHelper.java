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

import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class is meant to be used only by explicit Columns, and not directly to
 * write by clients.
 */
public final class ColumnHelper {

  private ColumnHelper() {
  }


  /**
   * @param columnPrefixBytes The byte representation for the column prefix.
   *          Should not contain {@link Separator#QUALIFIERS}.
   * @param qualifier for the remainder of the column.
   *          {@link Separator#QUALIFIERS} is permissible in the qualifier
   *          as it is joined only with the column prefix bytes.
   * @return fully sanitized column qualifier that is a combination of prefix
   *         and qualifier. If prefix is null, the result is simply the encoded
   *         qualifier without any separator.
   */
  public static byte[] getColumnQualifier(byte[] columnPrefixBytes,
      String qualifier) {

    // We don't want column names to have spaces / tabs.
    byte[] encodedQualifier =
        Separator.encode(qualifier, Separator.SPACE, Separator.TAB);
    if (columnPrefixBytes == null) {
      return encodedQualifier;
    }

    // Convert qualifier to lower case, strip of separators and tag on column
    // prefix.
    byte[] columnQualifier =
        Separator.QUALIFIERS.join(columnPrefixBytes, encodedQualifier);
    return columnQualifier;
  }

  /**
   * @param columnPrefixBytes The byte representation for the column prefix.
   *          Should not contain {@link Separator#QUALIFIERS}.
   * @param qualifier for the remainder of the column.
   * @return fully sanitized column qualifier that is a combination of prefix
   *         and qualifier. If prefix is null, the result is simply the encoded
   *         qualifier without any separator.
   */
  public static byte[] getColumnQualifier(byte[] columnPrefixBytes,
      long qualifier) {

    if (columnPrefixBytes == null) {
      return Bytes.toBytes(qualifier);
    }

    // Convert qualifier to lower case, strip of separators and tag on column
    // prefix.
    byte[] columnQualifier =
        Separator.QUALIFIERS.join(columnPrefixBytes, Bytes.toBytes(qualifier));
    return columnQualifier;
  }

  /**
   * @param columnPrefixBytes The byte representation for the column prefix.
   *          Should not contain {@link Separator#QUALIFIERS}.
   * @param qualifier the byte representation for the remainder of the column.
   * @return fully sanitized column qualifier that is a combination of prefix
   *         and qualifier. If prefix is null, the result is simply the encoded
   *         qualifier without any separator.
   */
  public static byte[] getColumnQualifier(byte[] columnPrefixBytes,
      byte[] qualifier) {

    if (columnPrefixBytes == null) {
      return qualifier;
    }

    byte[] columnQualifier =
        Separator.QUALIFIERS.join(columnPrefixBytes, qualifier);
    return columnQualifier;
  }

}
