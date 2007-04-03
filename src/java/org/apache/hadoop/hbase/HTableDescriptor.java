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

import org.apache.hadoop.io.*;

import java.io.*;
import java.util.*;

/*******************************************************************************
 * HTableDescriptor contains various facts about an HTable, like its columns, 
 * column families, etc.
 ******************************************************************************/
public class HTableDescriptor implements WritableComparable {
  Text name;
  int maxVersions;
  TreeSet<Text> families = new TreeSet<Text>();

  public HTableDescriptor() {
    this.name = new Text();
    this.families.clear();
  }

  public HTableDescriptor(String name, int maxVersions) {
    this.name = new Text(name);
    this.maxVersions = maxVersions;
  }

  public Text getName() {
    return name;
  }

  public int getMaxVersions() {
    return maxVersions;
  }

  /** Add a column */
  public void addFamily(Text family) {
    families.add(family);
  }

  /** Do we contain a given column? */
  public boolean hasFamily(Text family) {
    if(families.contains(family)) {
      return true;
      
    } else {
      return false;
    }
  }

  /** All the column families in this table. */
  public TreeSet<Text> families() {
    return families;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////////////////////////////////

  public void write(DataOutput out) throws IOException {
    name.write(out);
    out.writeInt(maxVersions);
    out.writeInt(families.size());
    for(Iterator<Text> it = families.iterator(); it.hasNext(); ) {
      it.next().write(out);
    }
  }

  public void readFields(DataInput in) throws IOException {
    this.name.readFields(in);
    this.maxVersions = in.readInt();
    int numCols = in.readInt();
    families.clear();
    for(int i = 0; i < numCols; i++) {
      Text t = new Text();
      t.readFields(in);
      families.add(t);
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // Comparable
  //////////////////////////////////////////////////////////////////////////////

  public int compareTo(Object o) {
    HTableDescriptor htd = (HTableDescriptor) o;
    int result = name.compareTo(htd.name);
    if(result == 0) {
      result = maxVersions - htd.maxVersions;
    }
    
    if(result == 0) {
      result = families.size() - htd.families.size();
    }
    
    if(result == 0) {
      Iterator<Text> it2 = htd.families.iterator();
      for(Iterator<Text> it = families.iterator(); it.hasNext(); ) {
        Text family1 = it.next();
        Text family2 = it2.next();
        result = family1.compareTo(family2);
        if(result != 0) {
          return result;
        }
      }
    }
    return result;
  }
}
