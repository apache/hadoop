/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm;

import java.util.Iterator;

/**
 * Iterator that can take current state of an existing iterator
 * and circularly iterate to that point.
 */
class CircularIterator<T> {
  private Iterator<T> iterator = null;
  private final Iterable<T> iterable;

  private T startElem = null;
  private T nextElem = null;

  // if not null, This overrides the starting Element.
  private T firstElem = null;

  // Can't handle empty or null lists.
  CircularIterator(T first, Iterator<T> iter,
      Iterable<T> iterable) {
    this.firstElem = first;
    this.iterable = iterable;
    if (!iter.hasNext()) {
      this.iterator = this.iterable.iterator();
    } else {
      this.iterator = iter;
    }
    this.startElem = this.iterator.next();
    this.nextElem = this.startElem;
  }

  boolean hasNext() {
    if (this.nextElem != null || this.firstElem != null) {
      return true;
    } else {
      if (this.iterator.hasNext()) {
        T next = this.iterator.next();
        if (this.startElem.equals(next)) {
          return false;
        } else {
          this.nextElem = next;
          return true;
        }
      } else {
        this.iterator = this.iterable.iterator();
        this.nextElem = this.iterator.next();
        if (this.startElem.equals(this.nextElem)) {
          return false;
        }
        return true;
      }
    }
  }

  T next() {
    T retVal;
    if (this.firstElem != null) {
      retVal = this.firstElem;
      this.firstElem = null;
    } else if (this.nextElem != null) {
      retVal = this.nextElem;
      this.nextElem = null;
    } else {
      retVal = this.iterator.next();
    }
    return retVal;
  }
}
