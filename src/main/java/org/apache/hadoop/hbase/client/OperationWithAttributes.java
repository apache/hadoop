/*
 * Copyright 2011 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.io.WritableUtils;

public abstract class OperationWithAttributes extends Operation implements Attributes {
  // a opaque blob of attributes
  private Map<String, byte[]> attributes;

  public void setAttribute(String name, byte[] value) {
    if (attributes == null && value == null) {
      return;
    }

    if (attributes == null) {
      attributes = new HashMap<String, byte[]>();
    }

    if (value == null) {
      attributes.remove(name);
      if (attributes.isEmpty()) {
        this.attributes = null;
      }
    } else {
      attributes.put(name, value);
    }
  }

  public byte[] getAttribute(String name) {
    if (attributes == null) {
      return null;
    }

    return attributes.get(name);
  }

  public Map<String, byte[]> getAttributesMap() {
    if (attributes == null) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(attributes);
  }

  protected long getAttributeSize() {
    long size = 0;
    if (attributes != null) {
      size += ClassSize.align(this.attributes.size() * ClassSize.MAP_ENTRY);
      for(Map.Entry<String, byte[]> entry : this.attributes.entrySet()) {
        size += ClassSize.align(ClassSize.STRING + entry.getKey().length());
        size += ClassSize.align(ClassSize.ARRAY + entry.getValue().length);
      }
    }
    return size;
  }

  protected void writeAttributes(final DataOutput out) throws IOException {
    if (this.attributes == null) {
      out.writeInt(0);
    } else {
      out.writeInt(this.attributes.size());
      for (Map.Entry<String, byte[]> attr : this.attributes.entrySet()) {
        WritableUtils.writeString(out, attr.getKey());
        Bytes.writeByteArray(out, attr.getValue());
      }
    }
  }
  
  protected void readAttributes(final DataInput in) throws IOException {
    int numAttributes = in.readInt();
    if (numAttributes > 0) {
      this.attributes = new HashMap<String, byte[]>();
      for(int i=0; i<numAttributes; i++) {
        String name = WritableUtils.readString(in);
        byte[] value = Bytes.readByteArray(in);
        this.attributes.put(name, value);
      }
    }
  }
}
