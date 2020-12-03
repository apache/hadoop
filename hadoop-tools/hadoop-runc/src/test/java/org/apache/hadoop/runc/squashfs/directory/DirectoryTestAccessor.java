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

package org.apache.hadoop.runc.squashfs.directory;

public final class DirectoryTestAccessor {

  private DirectoryTestAccessor() {
  }

  public static DirectoryHeader createDirectoryHeader() {
    return new DirectoryHeader();
  }

  public static DirectoryHeader createDirectoryHeader(
      int count, int startBlock, int inodeNumber) {
    return new DirectoryHeader(count, startBlock, inodeNumber);
  }

  public static DirectoryEntry createDirectoryEntry() {
    return new DirectoryEntry();
  }

  public static DirectoryEntry createDirectoryEntry(
      short offset, short inodeNumberDelta, short type, short size, byte[] name,
      DirectoryHeader header) {
    return new DirectoryEntry(
        offset, inodeNumberDelta, type, size, name, header);
  }

}
