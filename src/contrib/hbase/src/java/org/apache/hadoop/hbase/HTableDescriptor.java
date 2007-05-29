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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * HTableDescriptor contains the name of an HTable, and its
 * column families.
 */
public class HTableDescriptor implements WritableComparable {
  Text name;
  TreeMap<Text, HColumnDescriptor> families;
  
  /**
   * Legal table names can only contain 'word characters':
   * i.e. <code>[a-zA-Z_0-9]</code>.
   * 
   * Let's be restrictive until a reason to be otherwise.
   */
  private static final Pattern LEGAL_TABLE_NAME =
    Pattern.compile("[\\w-]+");
  
  public HTableDescriptor() {
    this.name = new Text();
    this.families = new TreeMap<Text, HColumnDescriptor>();
  }

  /**
   * Constructor.
   * @param name Table name.
   * @throws IllegalArgumentException if passed a table name
   * that is made of other than 'word' characters: i.e.
   * <code>[a-zA-Z_0-9]
   */
  public HTableDescriptor(String name) {
    Matcher m = LEGAL_TABLE_NAME.matcher(name);
    if (m == null || !m.matches()) {
      throw new IllegalArgumentException(
          "Table names can only contain 'word characters': i.e. [a-zA-Z_0-9");
    }
    this.name = new Text(name);
    this.families = new TreeMap<Text, HColumnDescriptor>();
  }

  public Text getName() {
    return name;
  }

  /**
   * Add a column family.
   * @param family HColumnDescriptor of familyto add.
   */
  public void addFamily(HColumnDescriptor family) {
    families.put(family.getName(), family);
  }

  /** Do we contain a given column? */
  public boolean hasFamily(Text family) {
    return families.containsKey(family);
  }

  /** All the column families in this table.
   * 
   *  TODO: What is this used for? Seems Dangerous to let people play with our
   *  private members.
   */
  public TreeMap<Text, HColumnDescriptor> families() {
    return families;
  }

  @Override
  public String toString() {
    return "name: " + this.name.toString() + ", families: " + this.families;
  }
  
  @Override
  public boolean equals(Object obj) {
    return compareTo(obj) == 0;
  }
  
  @Override
  public int hashCode() {
    // TODO: Cache.
    int result = this.name.hashCode();
    if (this.families != null && this.families.size() > 0) {
      for (Map.Entry<Text,HColumnDescriptor> e: this.families.entrySet()) {
        result ^= e.hashCode();
      }
    }
    return result;
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////////////////////////////////

  public void write(DataOutput out) throws IOException {
    name.write(out);
    out.writeInt(families.size());
    for(Iterator<HColumnDescriptor> it = families.values().iterator();
        it.hasNext(); ) {
      it.next().write(out);
    }
  }

  public void readFields(DataInput in) throws IOException {
    this.name.readFields(in);
    int numCols = in.readInt();
    families.clear();
    for(int i = 0; i < numCols; i++) {
      HColumnDescriptor c = new HColumnDescriptor();
      c.readFields(in);
      families.put(c.getName(), c);
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // Comparable
  //////////////////////////////////////////////////////////////////////////////

  public int compareTo(Object o) {
    HTableDescriptor other = (HTableDescriptor) o;
    int result = name.compareTo(other.name);
    
    if(result == 0) {
      result = families.size() - other.families.size();
    }
    
    if(result == 0 && families.size() != other.families.size()) {
      result = Integer.valueOf(families.size()).compareTo(
          Integer.valueOf(other.families.size()));
    }
    
    if(result == 0) {
      for(Iterator<HColumnDescriptor> it = families.values().iterator(),
          it2 = other.families.values().iterator(); it.hasNext(); ) {
        result = it.next().compareTo(it2.next());
        if(result != 0) {
          break;
        }
      }
    }
    return result;
  }
}