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
package org.apache.hadoop.hdfs.server.namenode;

/**
 * A generic interface for journal input and output streams.
 */
interface JournalStream {
  /**
   * Type of the underlying persistent storage type the stream is based upon.
   * <ul>
   * <li>{@link JournalType#FILE} - streams edits into a local file, see
   * {@link FSEditLog.EditLogFileOutputStream} and 
   * {@link FSEditLog.EditLogFileInputStream}</li>
   * <li>{@link JournalType#BACKUP} - streams edits to a backup node, see
   * {@link EditLogBackupOutputStream} and {@link EditLogBackupInputStream}</li>
   * </ul>
   */
  static enum JournalType {
    FILE,
    BACKUP;
    boolean isOfType(JournalType other) {
      return other == null || this == other;
    }
  };

  /**
   * Get this stream name.
   * 
   * @return name of the stream
   */
  String getName();

  /**
   * Get the type of the stream.
   * Determines the underlying persistent storage type.
   * @see JournalType
   * @return type
   */
  JournalType getType();
}
