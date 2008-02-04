/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase.hql;

import java.io.Writer;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.BloomFilterDescriptor;
import org.apache.hadoop.hbase.BloomFilterDescriptor.BloomFilterType;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.io.Text;

/**
 * The base class of schema modification commands, CreateCommand and Alter
 * Command. Provides utility methods for alteration operations.
 */
public abstract class SchemaModificationCommand extends BasicCommand {
  protected int maxVersions;
  protected int maxLength;
  protected HColumnDescriptor.CompressionType compression;
  protected boolean inMemory;
  protected boolean blockCacheEnabled;
  protected BloomFilterDescriptor bloomFilterDesc;
  protected BloomFilterType bloomFilterType;
  protected int vectorSize;
  protected int numHash;
  protected int numEntries;

  public SchemaModificationCommand(Writer o) {
    super(o);
  }

  protected void initOptions() {
    maxVersions = HColumnDescriptor.DEFAULT_N_VERSIONS;
    maxLength = HColumnDescriptor.DEFAULT_MAX_VALUE_LENGTH;
    compression = HColumnDescriptor.DEFAULT_COMPRESSION_TYPE;
    inMemory = HColumnDescriptor.DEFAULT_IN_MEMORY;
    blockCacheEnabled = HColumnDescriptor.DEFAULT_BLOCK_CACHE_ENABLED;
    bloomFilterDesc = HColumnDescriptor.DEFAULT_BLOOM_FILTER_DESCRIPTOR;
  }

  /**
   * Given a column name and column spec, returns an instance of
   * HColumnDescriptor representing the column spec.
   */
  protected HColumnDescriptor getColumnDescriptor(String column,
      Map<String, Object> columnSpec) throws IllegalArgumentException {
    initOptions();

    Set<String> specs = columnSpec.keySet();
    for (String spec : specs) {
      spec = spec.toUpperCase();

      if (spec.equals("MAX_VERSIONS")) {
        maxVersions = (Integer) columnSpec.get(spec);
      } else if (spec.equals("MAX_LENGTH")) {
        maxLength = (Integer) columnSpec.get(spec);
      } else if (spec.equals("COMPRESSION")) {
        compression = HColumnDescriptor.CompressionType
            .valueOf(((String) columnSpec.get(spec)).toUpperCase());
      } else if (spec.equals("IN_MEMORY")) {
        inMemory = (Boolean) columnSpec.get(spec);
      } else if (spec.equals("BLOCK_CACHE_ENABLED")) {
        blockCacheEnabled = (Boolean) columnSpec.get(spec);
      } else if (spec.equals("BLOOMFILTER")) {
        bloomFilterType = BloomFilterType.valueOf(((String) columnSpec.get(spec))
            .toUpperCase());
      } else if (spec.equals("VECTOR_SIZE")) {
        vectorSize = (Integer) columnSpec.get(spec);
      } else if (spec.equals("NUM_HASH")) {
        numHash = (Integer) columnSpec.get(spec);
      } else if (spec.equals("NUM_ENTRIES")) {
        numEntries = (Integer) columnSpec.get(spec);
      } else {
        throw new IllegalArgumentException("Invalid option: " + spec);
      }
    }

    // Now we gather all the specified options for this column.
    if (bloomFilterType != null) {
      if (specs.contains("NUM_ENTRIES")) {
        bloomFilterDesc = new BloomFilterDescriptor(bloomFilterType, numEntries);
      } else {
        bloomFilterDesc = new BloomFilterDescriptor(bloomFilterType, vectorSize,
            numHash);
      }
    }

    column = appendDelimiter(column);

    HColumnDescriptor columnDesc = new HColumnDescriptor(new Text(column),
        maxVersions, compression, inMemory, blockCacheEnabled,
        maxLength, bloomFilterDesc);

    return columnDesc;
  }
}
