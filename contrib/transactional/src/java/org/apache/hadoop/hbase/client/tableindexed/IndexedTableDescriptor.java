/**
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client.tableindexed;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

public class IndexedTableDescriptor {

  private static final byte[] INDEXES_KEY = Bytes.toBytes("INDEXES");

  private final HTableDescriptor baseTableDescriptor;
  // Key is indexId
  private final Map<String, IndexSpecification> indexes = new HashMap<String, IndexSpecification>();

  public IndexedTableDescriptor(HTableDescriptor baseTableDescriptor)
      throws IOException {
    this.baseTableDescriptor = baseTableDescriptor;
    readFromTable();
  }

  public HTableDescriptor getBaseTableDescriptor() {
    return this.baseTableDescriptor;
  }

  private void readFromTable() throws IOException {
    byte [] bytes = baseTableDescriptor.getValue(INDEXES_KEY);
    if (bytes == null) {
      return;
    }
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    DataInputStream dis = new DataInputStream(bais);
    IndexSpecificationArray indexArray = new IndexSpecificationArray();
    indexArray.readFields(dis);
    for (Writable index : indexArray.getIndexSpecifications()) {
      IndexSpecification indexSpec = (IndexSpecification) index;
      indexes.put(indexSpec.getIndexId(), indexSpec);
    }
  }

  private void writeToTable() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    IndexSpecificationArray indexArray = new IndexSpecificationArray(indexes.values().toArray(new IndexSpecification[0]));

    try {
      indexArray.write(dos);
      dos.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    baseTableDescriptor.setValue(INDEXES_KEY, baos.toByteArray());
  }

  public Collection<IndexSpecification> getIndexes() {
    return indexes.values();
  }

  public IndexSpecification getIndex(String indexId) {
    return indexes.get(indexId);
  }

  public void addIndex(IndexSpecification index) {
    indexes.put(index.getIndexId(), index);
    writeToTable();
  }

  public void removeIndex(String indexId) {
    indexes.remove(indexId);
    writeToTable();
  }

  @Override
  public String toString() {
    StringBuilder s = new StringBuilder(baseTableDescriptor.toString());

    if (!indexes.isEmpty()) {
      s.append(", ");
      s.append("INDEXES");
      s.append(" => ");
      s.append(indexes.values());
    }
    return s.toString();
  }
}
