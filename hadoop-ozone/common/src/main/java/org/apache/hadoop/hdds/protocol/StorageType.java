/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.protocol;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.StorageTypeProto;

/**
 * Ozone specific storage types.
 */
public enum StorageType {
  RAM_DISK,
  SSD,
  DISK,
  ARCHIVE;

  public static final StorageType DEFAULT = DISK;

  public StorageTypeProto toProto() {
    switch (this) {
    case DISK:
      return StorageTypeProto.DISK;
    case SSD:
      return StorageTypeProto.SSD;
    case ARCHIVE:
      return StorageTypeProto.ARCHIVE;
    case RAM_DISK:
      return StorageTypeProto.RAM_DISK;
    default:
      throw new IllegalStateException(
          "BUG: StorageType not found, type=" + this);
    }
  }

  public static StorageType valueOf(StorageTypeProto type) {
    switch (type) {
    case DISK:
      return DISK;
    case SSD:
      return SSD;
    case ARCHIVE:
      return ARCHIVE;
    case RAM_DISK:
      return RAM_DISK;
    default:
      throw new IllegalStateException(
          "BUG: StorageTypeProto not found, type=" + type);
    }
  }
}
