/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.security.access;

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.VersionedWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * Base permissions instance representing the ability to perform a given set
 * of actions.
 *
 * @see TablePermission
 */
public class Permission extends VersionedWritable {
  protected static final byte VERSION = 0;
  public enum Action {
    READ('R'), WRITE('W'), EXEC('X'), CREATE('C'), ADMIN('A');

    private byte code;
    Action(char code) {
      this.code = (byte)code;
    }

    public byte code() { return code; }
  }

  private static Log LOG = LogFactory.getLog(Permission.class);
  protected static Map<Byte,Action> ACTION_BY_CODE = Maps.newHashMap();

  protected Action[] actions;

  static {
    for (Action a : Action.values()) {
      ACTION_BY_CODE.put(a.code(), a);
    }
  }

  /** Empty constructor for Writable implementation.  <b>Do not use.</b> */
  public Permission() {
    super();
  }

  public Permission(Action... assigned) {
    if (assigned != null && assigned.length > 0) {
      actions = Arrays.copyOf(assigned, assigned.length);
    }
  }

  public Permission(byte[] actionCodes) {
    if (actionCodes != null) {
      Action acts[] = new Action[actionCodes.length];
      int j = 0;
      for (int i=0; i<actionCodes.length; i++) {
        byte b = actionCodes[i];
        Action a = ACTION_BY_CODE.get(b);
        if (a == null) {
          LOG.error("Ignoring unknown action code '"+
              Bytes.toStringBinary(new byte[]{b})+"'");
          continue;
        }
        acts[j++] = a;
      }
      this.actions = Arrays.copyOf(acts, j);
    }
  }

  public Action[] getActions() {
    return actions;
  }

  public boolean implies(Action action) {
    if (this.actions != null) {
      for (Action a : this.actions) {
        if (a == action) {
          return true;
        }
      }
    }

    return false;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Permission)) {
      return false;
    }
    Permission other = (Permission)obj;
    // check actions
    if (actions == null && other.getActions() == null) {
      return true;
    } else if (actions != null && other.getActions() != null) {
      Action[] otherActions = other.getActions();
      if (actions.length != otherActions.length) {
        return false;
      }

      outer:
      for (Action a : actions) {
        for (Action oa : otherActions) {
          if (a == oa) continue outer;
        }
        return false;
      }
      return true;
    }

    return false;
  }

  @Override
  public int hashCode() {
    final int prime = 37;
    int result = 23;
    for (Action a : actions) {
      result = prime * result + a.code();
    }
    return result;
  }

  public String toString() {
    StringBuilder str = new StringBuilder("[Permission: ")
        .append("actions=");
    if (actions != null) {
      for (int i=0; i<actions.length; i++) {
        if (i > 0)
          str.append(",");
        if (actions[i] != null)
          str.append(actions[i].toString());
        else
          str.append("NULL");
      }
    }
    str.append("]");

    return str.toString();
  }

  /** @return the object version number */
  public byte getVersion() {
    return VERSION;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    int length = (int)in.readByte();
    if (length > 0) {
      actions = new Action[length];
      for (int i = 0; i < length; i++) {
        byte b = in.readByte();
        Action a = ACTION_BY_CODE.get(b);
        if (a == null) {
          throw new IOException("Unknown action code '"+
              Bytes.toStringBinary(new byte[]{b})+"' in input");
        }
        this.actions[i] = a;
      }
    } else {
      actions = new Action[0];
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeByte(actions != null ? actions.length : 0);
    if (actions != null) {
      for (Action a: actions) {
        out.writeByte(a.code());
      }
    }
  }
}
