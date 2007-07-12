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
package org.apache.hadoop.hbase;

/**
 * Global values used for finding and scanning the root and meta tables.
 */
public class HGlobals implements HConstants {
  
  static HTableDescriptor rootTableDesc = null;
  static HRegionInfo rootRegionInfo = null;
  static HTableDescriptor metaTableDesc = null;

  static {
    rootTableDesc = new HTableDescriptor(ROOT_TABLE_NAME.toString());
    rootTableDesc.addFamily(new HColumnDescriptor(COLUMN_FAMILY, 1,
        HColumnDescriptor.CompressionType.NONE, false, Integer.MAX_VALUE, null));
    
    rootRegionInfo = new HRegionInfo(0L, rootTableDesc, null, null);
    
    metaTableDesc = new HTableDescriptor(META_TABLE_NAME.toString());
    metaTableDesc.addFamily(new HColumnDescriptor(COLUMN_FAMILY, 1,
        HColumnDescriptor.CompressionType.NONE, false, Integer.MAX_VALUE, null));
  }
}
