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
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * HTableDescriptor contains various facts about an HTable, like
 * column families, maximum number of column versions, etc.
 */
public class HTableDescriptor implements WritableComparable {
  Text name;
  int maxVersions;
  TreeSet<Text> families = new TreeSet<Text>();
  
  /**
   * Legal table names can only contain 'word characters':
   * i.e. <code>[a-zA-Z_0-9]</code>.
   * 
   * Lets be restrictive until a reason to be otherwise.
   */
  private static final Pattern LEGAL_TABLE_NAME =
    Pattern.compile("[\\w-]+");
  
  /**
   * Legal family names can only contain 'word characters' and
   * end in a colon.
   */
  private static final Pattern LEGAL_FAMILY_NAME =
    Pattern.compile("\\w+:");

  public HTableDescriptor() {
    this.name = new Text();
    this.families.clear();
  }

  /**
   * Constructor.
   * @param name Table name.
   * @param maxVersions Number of versions of a column to keep.
   * @throws IllegalArgumentException if passed a table name
   * that is made of other than 'word' characters: i.e.
   * <code>[a-zA-Z_0-9]
   */
  public HTableDescriptor(String name, int maxVersions) {
    Matcher m = LEGAL_TABLE_NAME.matcher(name);
    if (m == null || !m.matches()) {
      throw new IllegalArgumentException("Table names can only " +
          "contain 'word characters': i.e. [a-zA-Z_0-9");
    }
    if (maxVersions <= 0) {
      // TODO: Allow maxVersion of 0 to be the way you say
      // "Keep all versions".  Until there is support, consider
      // 0 -- or < 0 -- a configuration error.
      throw new IllegalArgumentException("Maximum versions " +
        "must be positive");
    }
    this.name = new Text(name);
    this.maxVersions = maxVersions;
  }

  public Text getName() {
    return name;
  }

  public int getMaxVersions() {
    return maxVersions;
  }

  /**
   * Add a column family.
   * @param family Column family name to add.  Column family names
   * must end in a <code>:</code>
   * @throws IllegalArgumentException if passed a table name
   * that is made of other than 'word' characters: i.e.
   * <code>[a-zA-Z_0-9]
   */
  public void addFamily(Text family) {
    String familyStr = family.toString();
    Matcher m = LEGAL_FAMILY_NAME.matcher(familyStr);
    if (m == null || !m.matches()) {
      throw new IllegalArgumentException("Family names can " +
          "only contain 'word characters' and must end with a " +
          "':'");
    }
    families.add(family);
  }

  /** Do we contain a given column? */
  public boolean hasFamily(Text family) {
    return families.contains(family);
  }

  /** All the column families in this table. */
  public TreeSet<Text> families() {
    return families;
  }

  @Override
  public String toString() {
    return "name: " + this.name.toString() +
      ", maxVersions: " + this.maxVersions + ", families: " + this.families;
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