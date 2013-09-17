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

package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * <code>MarkableIterator</code> is a wrapper iterator class that 
 * implements the {@link MarkableIteratorInterface}.
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MarkableIterator<VALUE> 
  implements MarkableIteratorInterface<VALUE> {

  MarkableIteratorInterface<VALUE> baseIterator;

  /**
   * Create a new iterator layered on the input iterator
   * @param itr underlying iterator that implements MarkableIteratorInterface
   */
  public MarkableIterator(Iterator<VALUE> itr)  {
    if (!(itr instanceof MarkableIteratorInterface)) {
      throw new IllegalArgumentException("Input Iterator not markable");
    }
    baseIterator = (MarkableIteratorInterface<VALUE>) itr;
  }

  @Override
  public void mark() throws IOException {
    baseIterator.mark();
  }

  @Override
  public void reset() throws IOException {
    baseIterator.reset();
  }

  @Override
  public void clearMark() throws IOException {
    baseIterator.clearMark();
  }

  @Override
  public boolean hasNext() { 
    return baseIterator.hasNext();
  }

  @Override
  public VALUE next() {
    return baseIterator.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Remove Not Implemented");
  }
}
