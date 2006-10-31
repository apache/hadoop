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

package org.apache.hadoop.fs;

import java.util.*;

/** Names a file or directory in a {@link FileSystem}.
 * Path strings use slash as the directory separator.  A path string is
 * absolute if it begins with a slash.
 */
public class Path implements Comparable {

  /** The directory separator, a slash. */
  public static final String SEPARATOR = "/";
  
  static final boolean WINDOWS
    = System.getProperty("os.name").startsWith("Windows");

  private boolean isAbsolute;                     // if path starts with sepr
  private String[] elements;                      // tokenized path elements
  private String drive;                           // Windows drive letter
  private String asString;                        // cached toString() value

  /** Resolve a child path against a parent path. */
  public Path(String parent, String child) {
    this(new Path(parent), new Path(child));
  }

  /** Resolve a child path against a parent path. */
  public Path(Path parent, String child) {
    this(parent, new Path(child));
  }

  /** Resolve a child path against a parent path. */
  public Path(String parent, Path child) {
    this(new Path(parent), child);
  }

  /** Resolve a child path against a parent path. */
  public Path(Path parent, Path child) {
    if (child.isAbsolute()) {
      this.isAbsolute = child.isAbsolute;
      this.elements = child.elements;
    } else {
      this.isAbsolute = parent.isAbsolute;
      ArrayList list = new ArrayList(parent.elements.length+child.elements.length);
      for (int i = 0; i < parent.elements.length; i++) {
        list.add(parent.elements[i]);
      }
      for (int i = 0; i < child.elements.length; i++) {
        list.add(child.elements[i]);
      }
      normalize(list);
      this.elements = (String[])list.toArray(new String[list.size()]);
    }
    this.drive = child.drive == null ? parent.drive : child.drive;
  }

  /** Construct a path from a String. */
  public Path(String pathString) {
    if (WINDOWS) {                                // parse Windows path
      int colon = pathString.indexOf(':');
      if (colon == 1) {                           // parse Windows drive letter
        this.drive = pathString.substring(0, 1);
        pathString = pathString.substring(2);
      }
      pathString = pathString.replace('\\','/');  // convert backslash to slash
    }

    // determine whether the path is absolute
    this.isAbsolute = pathString.startsWith(SEPARATOR);

    // tokenize the path into elements
    Enumeration tokens = new StringTokenizer(pathString, SEPARATOR);    
    ArrayList list = Collections.list(tokens);
    normalize(list);
    this.elements = (String[])list.toArray(new String[list.size()]);
  }

  private Path(boolean isAbsolute, String[] elements, String drive) {
    this.isAbsolute = isAbsolute;
    this.elements = elements;
    this.drive = drive;
  }

  /** True if this path is absolute. */
  public boolean isAbsolute() { return isAbsolute; }

  /** Returns the final component of this path.*/
  public String getName() {
    if (elements.length == 0) {
      return "";
    } else {
      return elements[elements.length-1];
    }
  }

  /** Returns the parent of a path. */
  public Path getParent() {
    if (elements.length  == 0) {
      return null;
    }
    String[] newElements = new String[elements.length-1];
    for (int i = 0; i < newElements.length; i++) {
      newElements[i] = elements[i];
    }
    return new Path(isAbsolute, newElements, drive);
  }

  /** Adds a suffix to the final name in the path.*/
  public Path suffix(String suffix) {
    return new Path(getParent(), getName()+suffix);
  }

  public String toString() {
    if (asString == null) {
      StringBuffer buffer = new StringBuffer();

      if (drive != null) {
        buffer.append(drive);
        buffer.append(':');
      }

      if (elements.length == 0 && isAbsolute) {
        buffer.append(SEPARATOR);
      }

      for (int i = 0; i < elements.length; i++) {
        if (i !=0 || isAbsolute) {
          buffer.append(SEPARATOR);
        }
        buffer.append(elements[i]);
      }
      asString = buffer.toString();
    }
    return asString;
  }

  public boolean equals(Object o) {
    if (!(o instanceof Path)) {
      return false;
    }
    Path that = (Path)o;
    return
      this.isAbsolute == that.isAbsolute &&
      Arrays.equals(this.elements, that.elements) &&
      (this.drive == null ? true : this.drive.equals(that.drive));
  }

  public int hashCode() {
    int hashCode = isAbsolute ? 1 : -1;
    for (int i = 0; i < elements.length; i++) {
      hashCode ^= elements[i].hashCode();
    }
    if (drive != null) {
      hashCode ^= drive.hashCode();
    }
    return hashCode;
  }

  public int compareTo(Object o) {
    Path that = (Path)o;
    return this.toString().compareTo(that.toString());
  }
  
  /** Return the number of elements in this path. */
  public int depth() {
    return elements.length;
  }

  /* 
   * Removes '.' and '..' 
   */
  private void normalize(ArrayList list) {
    boolean canNormalize = this.isAbsolute;
    boolean changed = false;    // true if we have detected any . or ..
    int index = 0;
    int listSize = list.size();
    for (int i = 0; i < listSize; i++) {
      // Invariant: (index >= 0) && (index <= i)
      if (list.get(i).equals(".")) {
        changed = true;
      } else {
        if (canNormalize) {
          if (list.get(i).equals("..")) {
            if ((index > 0) && !list.get(index-1).equals("..")) {
              index--;    // effectively deletes the last element currently in list.
              changed = true;
            } else { // index == 0
              // the first element is now going to be '..'
              canNormalize = false;
              list.set(index, "..");
              isAbsolute = false;
              index++; 
            }
          } else { // list.get(i) != ".." or "."
            if (changed) {
              list.set(index, list.get(i));
            }
            index++;
          }
        } else { // !canNormalize
          if (changed) {
            list.set(index, list.get(i));
          }
          index++;
          if (!list.get(i).equals("..")) {
           canNormalize = true;
          }
        }  // else !canNormalize
      } 
    }  // for
    
    // Remove the junk at the end of the list.
    for (int j = listSize-1; j >= index; j--) {
      list.remove(j);
    }

  }
  
  
}

