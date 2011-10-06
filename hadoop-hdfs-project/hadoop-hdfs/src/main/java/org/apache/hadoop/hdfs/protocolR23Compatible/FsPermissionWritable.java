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
package org.apache.hadoop.hdfs.protocolR23Compatible;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class FsPermissionWritable  implements Writable {
  static final WritableFactory FACTORY = new WritableFactory() {
	public Writable newInstance() { return new FsPermissionWritable(); }
  };
  static {                                      // register a ctor
    WritableFactories.setFactory(FsPermissionWritable.class, FACTORY);
  }
  //POSIX permission style
  private short thePermissions = 0;
  
  public static FsPermissionWritable convertPermission(org.apache.hadoop.fs.permission.FsPermission p) {
    if (p == null) return null;
    return new FsPermissionWritable(p.toShort());
  }
  
  public static org.apache.hadoop.fs.permission.FsPermission convertPermission(FsPermissionWritable p) {
    if (p == null) return null;
    return new org.apache.hadoop.fs.permission.FsPermission(p.thePermissions);
  }
  
  public static FsPermissionWritable getDefault() {
    return new FsPermissionWritable((short)00777);
  }
  
  FsPermissionWritable() {
  }
	FsPermissionWritable(short p) {
	  thePermissions = p;
	}
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeShort(thePermissions);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    thePermissions = in.readShort();
  }

  /**
   * Create and initialize a {@link FsPermissionWritable} from {@link DataInput}.
   */
  public static FsPermissionWritable read(DataInput in) throws IOException {
    FsPermissionWritable p = new FsPermissionWritable();
    p.readFields(in);
    return p;
  }
}
