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

package org.apache.hadoop.runc.squashfs.inode;

import java.util.EnumSet;

public enum Permission {
  S_IXOTH(0, "execute (other)"),
  S_IWOTH(1, "write (other"),
  S_IROTH(2, "read (other)"),
  S_IXGRP(3, "execute (group)"),
  S_IWGRP(4, "write (group)"),
  S_IRGRP(5, "read (group)"),
  S_IXUSR(6, "execute (user)"),
  S_IWUSR(7, "write (user)"),
  S_IRUSR(8, "read (user)"),
  S_ISVTX(9, "restricted delete"),
  S_ISGID(10, "set gid"),
  S_ISUID(11, "set uid");

  private final String description;
  private final int mask;
  private final int bit;

  Permission(int bit, String description) {
    this.bit = bit;
    this.mask = (1 << bit);
    this.description = description;
  }

  public static EnumSet<Permission> from(int value) {
    EnumSet<Permission> result = EnumSet.noneOf(Permission.class);
    for (Permission perm : values()) {
      if ((value & perm.mask) == perm.mask) {
        result.add(perm);
      }
    }
    return result;
  }

  public static EnumSet<Permission> from(String display)
      throws IllegalArgumentException {
    return from(toValue(display));
  }

  public static int toValue(EnumSet<Permission> perms) {
    int result = 0;
    for (Permission perm : perms) {
      result |= perm.mask;
    }
    return result;
  }

  public static String toDisplay(int value) {
    char[] c = new char[9];

    c[0] = toDisplay(value, S_IRUSR.mask, 'r', '-');
    c[1] = toDisplay(value, S_IWUSR.mask, 'w', '-');
    c[2] = toDisplay(value, S_IXUSR.mask, S_ISUID.mask, 'x', '-', 's', 'S');
    c[3] = toDisplay(value, S_IRGRP.mask, 'r', '-');
    c[4] = toDisplay(value, S_IWGRP.mask, 'w', '-');
    c[5] = toDisplay(value, S_IXGRP.mask, S_ISGID.mask, 'x', '-', 's', 'S');
    c[6] = toDisplay(value, S_IROTH.mask, 'r', '-');
    c[7] = toDisplay(value, S_IWOTH.mask, 'w', '-');
    c[8] = toDisplay(value, S_IXOTH.mask, S_ISVTX.mask, 'x', '-', 't', 'T');

    return new String(c);
  }

  public static int toValue(String display) {
    if (display.length() < 9) {
      throw new IllegalArgumentException(
          String.format("Invalid permission string %s", display));
    }
    int result = 0;
    char[] c = display.toCharArray();

    // standard bits
    result |= setBitIf(c[0], 'r', S_IRUSR.mask);
    result |= setBitIf(c[1], 'w', S_IWUSR.mask);
    result |= setBitIf(c[2], 'x', S_IXUSR.mask);
    result |= setBitIf(c[3], 'r', S_IRGRP.mask);
    result |= setBitIf(c[4], 'w', S_IWGRP.mask);
    result |= setBitIf(c[5], 'x', S_IXGRP.mask);
    result |= setBitIf(c[6], 'r', S_IROTH.mask);
    result |= setBitIf(c[7], 'w', S_IWOTH.mask);
    result |= setBitIf(c[8], 'x', S_IXOTH.mask);

    // extended bits
    result |= setBitIf(c[2], 's', S_IXUSR.mask | S_ISUID.mask);
    result |= setBitIf(c[2], 'S', S_ISUID.mask);
    result |= setBitIf(c[5], 's', S_IXGRP.mask | S_ISGID.mask);
    result |= setBitIf(c[5], 'S', S_ISGID.mask);
    result |= setBitIf(c[8], 't', S_IXOTH.mask | S_ISVTX.mask);
    result |= setBitIf(c[8], 'T', S_ISVTX.mask);

    return result;
  }

  public static String toDisplay(EnumSet<Permission> perms) {
    return toDisplay(toValue(perms));
  }

  private static char toDisplay(int value, int mask, char yes, char no) {
    return ((value & mask) == mask) ? yes : no;
  }

  private static char toDisplay(int value, int mask1, int mask2, char c1,
      char c0, char c12, char c2) {
    int both = (mask1 | mask2);

    if ((value & both) == mask1) {
      return c1;
    } else if ((value & both) == mask2) {
      return c2;
    } else if ((value & both) == both) {
      return c12;
    } else {
      return c0;
    }
  }

  private static int setBitIf(char c, char t, int value) {
    return (c == t) ? value : 0;
  }

  public String description() {
    return description;
  }

  public int mask() {
    return mask;
  }

  public int bit() {
    return bit;
  }

}
