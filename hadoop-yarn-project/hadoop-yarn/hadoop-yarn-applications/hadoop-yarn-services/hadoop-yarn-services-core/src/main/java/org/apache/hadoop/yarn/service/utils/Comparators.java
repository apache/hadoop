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

package org.apache.hadoop.yarn.service.utils;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Some general comparators
 */
public class Comparators {

  public static class LongComparator implements Comparator<Long>, Serializable {
    @Override
    public int compare(Long o1, Long o2) {
      return o1.compareTo(o2);
    }
  }

  public static class InvertedLongComparator
      implements Comparator<Long>, Serializable {
    @Override
    public int compare(Long o1, Long o2) {
      return o2.compareTo(o1);
    }
  }

  /**
   * Little template class to reverse any comparitor
   * @param <CompareType> the type that is being compared
   */
  public static class ComparatorReverser<CompareType> implements Comparator<CompareType>,
      Serializable {

    final Comparator<CompareType> instance;

    public ComparatorReverser(Comparator<CompareType> instance) {
      this.instance = instance;
    }

    @Override
    public int compare(CompareType first, CompareType second) {
      return instance.compare(second, first);
    }
  }
}
