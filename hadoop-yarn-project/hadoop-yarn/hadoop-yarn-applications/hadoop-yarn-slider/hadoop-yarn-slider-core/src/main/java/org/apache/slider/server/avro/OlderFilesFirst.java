/*
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

package org.apache.slider.server.avro;

import org.apache.hadoop.fs.Path;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Compare two filenames by name; the older ones comes first
 */
public class OlderFilesFirst implements Comparator<Path>, Serializable {

  /**
   * Takes the ordering of path names from the normal string comparison
   * and negates it, so that names that come after other names in 
   * the string sort come before here
   * @param o1 leftmost 
   * @param o2 rightmost
   * @return positive if o1 &gt; o2 
   */
  @Override
  public int compare(Path o1, Path o2) {
    return (o1.getName().compareTo(o2.getName()));
  }
}
