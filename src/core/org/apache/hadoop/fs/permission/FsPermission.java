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
package org.apache.hadoop.fs.permission;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A class for file/directory permissions.
 */
public class FsPermission implements Writable {
  static final WritableFactory FACTORY = new WritableFactory() {
    public Writable newInstance() { return new FsPermission(); }
  };
  static {                                      // register a ctor
    WritableFactories.setFactory(FsPermission.class, FACTORY);
  }

  /** Create an immutable {@link FsPermission} object. */
  public static FsPermission createImmutable(short permission) {
    return new FsPermission(permission) {
      public FsPermission applyUMask(FsPermission umask) {
        throw new UnsupportedOperationException();
      }
      public void readFields(DataInput in) throws IOException {
        throw new UnsupportedOperationException();
      }
    };
  }

  //POSIX permission style
  private FsAction useraction = null;
  private FsAction groupaction = null;
  private FsAction otheraction = null;

  private FsPermission() {}

  /**
   * Construct by the given {@link FsAction}.
   * @param u user action
   * @param g group action
   * @param o other action
   */
  public FsPermission(FsAction u, FsAction g, FsAction o) {set(u, g, o);}

  /**
   * Construct by the given mode.
   * @param mode
   * @see #toShort()
   */
  public FsPermission(short mode) { fromShort(mode); }

  /**
   * Copy constructor
   * 
   * @param other other permission
   */
  public FsPermission(FsPermission other) {
    this.useraction = other.useraction;
    this.groupaction = other.groupaction;
    this.otheraction = other.otheraction;
  }
  
  /** Return user {@link FsAction}. */
  public FsAction getUserAction() {return useraction;}

  /** Return group {@link FsAction}. */
  public FsAction getGroupAction() {return groupaction;}

  /** Return other {@link FsAction}. */
  public FsAction getOtherAction() {return otheraction;}

  private void set(FsAction u, FsAction g, FsAction o) {
    useraction = u;
    groupaction = g;
    otheraction = o;
  }
  public void fromShort(short n) {
    FsAction[] v = FsAction.values();
    set(v[(n >>> 6) & 7], v[(n >>> 3) & 7], v[n & 7]);
  }

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    out.writeShort(toShort());
  }

  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    fromShort(in.readShort());
  }

  /**
   * Create and initialize a {@link FsPermission} from {@link DataInput}.
   */
  public static FsPermission read(DataInput in) throws IOException {
    FsPermission p = new FsPermission();
    p.readFields(in);
    return p;
  }

  /**
   * Encode the object to a short.
   */
  public short toShort() {
    int s = (useraction.ordinal() << 6) | (groupaction.ordinal() << 3) |
             otheraction.ordinal();
    return (short)s;
  }

  /** {@inheritDoc} */
  public boolean equals(Object obj) {
    if (obj instanceof FsPermission) {
      FsPermission that = (FsPermission)obj;
      return this.useraction == that.useraction
          && this.groupaction == that.groupaction
          && this.otheraction == that.otheraction;
    }
    return false;
  }

  /** {@inheritDoc} */
  public int hashCode() {return toShort();}

  /** {@inheritDoc} */
  public String toString() {
    return useraction.SYMBOL + groupaction.SYMBOL + otheraction.SYMBOL;
  }

  /** Apply a umask to this permission and return a new one */
  public FsPermission applyUMask(FsPermission umask) {
    return new FsPermission(useraction.and(umask.useraction.not()),
        groupaction.and(umask.groupaction.not()),
        otheraction.and(umask.otheraction.not()));
  }

  /** umask property label */
  public static final String UMASK_LABEL = "dfs.umask";
  public static final int DEFAULT_UMASK = 0022;

  /** Get the user file creation mask (umask) */
  public static FsPermission getUMask(Configuration conf) {
    int umask = DEFAULT_UMASK;
    if (conf != null) {
      umask = conf.getInt(UMASK_LABEL, DEFAULT_UMASK);
    }
    return new FsPermission((short)umask);
  }
  /** Set the user file creation mask (umask) */
  public static void setUMask(Configuration conf, FsPermission umask) {
    conf.setInt(UMASK_LABEL, umask.toShort());
  }

  /** Get the default permission. */
  public static FsPermission getDefault() {
    return new FsPermission((short)0777);
  }

  /**
   * Create a FsPermission from a Unix symbolic permission string
   * @param unixSymbolicPermission e.g. "-rw-rw-rw-"
   */
  public static FsPermission valueOf(String unixSymbolicPermission) {
    if (unixSymbolicPermission == null) {
      return null;
    }
    else if (unixSymbolicPermission.length() != 10) {
      throw new IllegalArgumentException("length != 10(unixSymbolicPermission="
          + unixSymbolicPermission + ")");
    }
    int n = 0;
    for(int i = 1; i < unixSymbolicPermission.length(); i++) {
      n = n << 1;
      char c = unixSymbolicPermission.charAt(i);
      n += (c == '-' || c == 'T' || c == 'S') ? 0: 1;
    }
    return new FsPermission((short)n);
  }
}
