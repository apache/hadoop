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

package org.apache.hadoop.raid.protocol;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Collection;
import java.util.List;
import java.util.LinkedList;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

/**
 * Maintains informatiom about all policies that belong to a category.
 * These policies have to be applied one-at-a-time and cannot be run
 * simultaneously.
 */
public class PolicyList implements Writable {
  public static final Log LOG = LogFactory.getLog(
    "org.apache.hadoop.raid.protocol.PolicyList");

  private List<PolicyInfo> category; // list of policies
  private Path srcPath;
  
  /**
   * Create a new category of policies.
   */
  public PolicyList() {
    this.category = new LinkedList<PolicyInfo>();
    this.srcPath = null;
  }

  /**
   * Add a new policy to this category.
   */
  public void add(PolicyInfo info) {
    category.add(info); 
  }

  public void setSrcPath(Configuration conf, String src) throws IOException {
    srcPath = new Path(src);
    srcPath = srcPath.makeQualified(srcPath.getFileSystem(conf));
  }
  
  public Path getSrcPath() {
    return srcPath;
  }
  
  /**
   * Returns the policies in this category
   */
  public Collection<PolicyInfo> getAll() {
    return category;
  }

  /**
   * Sort Categries based on their srcPath. reverse lexicographical order.
   */
  public static class CompareByPath implements Comparator<PolicyList> {
    public CompareByPath() throws IOException {
    }
    public int compare(PolicyList l1, PolicyList l2) {
      return 0 - l1.getSrcPath().compareTo(l2.getSrcPath());
    }
  }
  
  
  //////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
      (PolicyList.class,
       new WritableFactory() {
         public Writable newInstance() { return new PolicyList(); }
       });
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(category.size());
    for (PolicyInfo p : category) {
      p.write(out);
    }
  }

  public void readFields(DataInput in) throws IOException {
    int count = in.readInt();
    for (int i = 0; i < count; i++) {
      PolicyInfo p = new PolicyInfo();
      p.readFields(in);
      add(p);
    }
  }
}
