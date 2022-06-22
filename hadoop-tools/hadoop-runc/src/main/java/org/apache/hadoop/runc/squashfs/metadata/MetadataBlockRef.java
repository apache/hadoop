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

package org.apache.hadoop.runc.squashfs.metadata;

import org.apache.hadoop.runc.squashfs.inode.INodeRef;

public class MetadataBlockRef {

  private final int location;
  private final short offset;

  public MetadataBlockRef(int location, short offset) {
    this.location = location;
    this.offset = offset;
  }

  public int getLocation() {
    return location;
  }

  public short getOffset() {
    return offset;
  }

  @Override
  public String toString() {
    return String.format(
        "metadata-block-ref { location=%d, offset=%d }",
        location,
        offset);
  }

  public INodeRef toINodeRef() {
    return new INodeRef(location, offset);
  }

  public long toINodeRefRaw() {
    return ((long) (location & 0xffffffffL) << 16) | ((long) (offset
        & 0xffffL));
  }

}
