/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

/*******************************************************************************
 * Global values used for finding and scanning the root and meta tables.
 ******************************************************************************/
public class HGlobals implements HConstants {
  
  static HTableDescriptor rootTableDesc = null;
  static HRegionInfo rootRegionInfo = null;
  static HTableDescriptor metaTableDesc = null;

  static {
    rootTableDesc = new HTableDescriptor(ROOT_TABLE_NAME.toString(), 1);
    rootTableDesc.addFamily(ROOT_COLUMN_FAMILY);
    
    rootRegionInfo = new HRegionInfo(0L, rootTableDesc, null, null);
    
    metaTableDesc = new HTableDescriptor(META_TABLE_NAME.toString(), 1);
    metaTableDesc.addFamily(META_COLUMN_FAMILY);
  }
  

}
