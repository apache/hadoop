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
package org.apache.hadoop.util;

/**
 * An implementation of the core algorithm of QuickSort.
 * See "Median-of-Three Partitioning" in Sedgewick book.
 */
public class QuickSort implements IndexedSorter {

  public QuickSort() { }

  private void fix(IndexedSortable s, int p, int r) {
    if (s.compare(p, r) > 0) {
      s.swap(p, r);
    }
  }

  public void sort(IndexedSortable s, int p, int r) {
    sort(s, p, r, null);
  }

  /**
   * Same as {@link #sort}, but indicate that we're making progress after
   * each partition.
   */
  public void sort(IndexedSortable s, int p, int r, Progressable rep) {
    if (null != rep) {
      rep.progress();
    }
    while (true) {
    if (r-p < 13) {
      for (int i = p; i < r; ++i) {
        for (int j = i; j > p; --j) {
          if (s.compare(j-1, j) > 0) {
            s.swap(j, j-1);
          }
        }
      }
      return;
    }

    // select, move pivot into first position
    fix(s, (p+r) >>> 1, p);
    fix(s, (p+r) >>> 1, r - 1);
    fix(s, p, r-1);

    // Divide
    int x = p;
    int i = p;
    int j = r;
    while(true) {
      while (++i < r && s.compare(i, x) < 0) { } // move lindex
      while (--j > x && s.compare(x, j) < 0) { } // move rindex
      if (i < j) s.swap(i, j);
      else break;
    }
    // swap pivot into position
    s.swap(x, i - 1);

    // Conquer
    // Recurse on smaller interval first to keep stack shallow
    if (i - p - 1 < r - i) {
      sort(s, p, i - 1, rep);
      p = i;
    } else {
      sort(s, i, r, rep);
      r = i - 1;
    }
    }
  }

}
