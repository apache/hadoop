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
package org.apache.hadoop.yarn.api.records;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.yarn.util.Records;

public abstract class ValueRanges implements Comparable<ValueRanges> {

  public static ValueRanges newInstance(List<ValueRange> rangesList) {
    ValueRanges valueRanges = Records.newRecord(ValueRanges.class);
    valueRanges.setRangesList(rangesList);
    return valueRanges;
  }

  public static ValueRanges newInstance() {
    ValueRanges valueRanges = Records.newRecord(ValueRanges.class);
    return valueRanges;
  }

  public abstract List<ValueRange> getRangesList();

  public abstract List<ValueRange> getSortedRangesList();

  public abstract void setRangesList(List<ValueRange> rangesList);

  public abstract BitSet getBitSetStore();

  public abstract void setBitSetStore(BitSet bitSetStore);

  public abstract boolean isByteStoreEnable();

  public abstract void setByteStoreEnable(boolean enable);

  public abstract ByteBuffer getBytesStore();

  @Override
  public String toString() {
    BitSet bitSetStore = this.getBitSetStore();
    List<String> list = new ArrayList<>();

    if (bitSetStore == null) {
      for (ValueRange range : getSortedRangesList()) {
        list.add(range.toString());
      }
    } else {
      for (int start = bitSetStore.nextSetBit(0); start >= 0;) {
        int end = bitSetStore.nextClearBit(start) - 1;
        list.add("[" + start + "-" + end + "]");
        start = bitSetStore.nextSetBit(end + 1);
      }
    }

    return String.join(",", list);
  }

  public static ValueRanges convertToBitSet(ValueRanges original) {
    ValueRanges result = ValueRanges.newInstance();
    BitSet bitSetStore = new BitSet();

    if (original != null) {
      if (original.getBitSetStore() != null) {
        bitSetStore = original.getBitSetStore();
      } else {
        if (original.isByteStoreEnable() && original.getBytesStore() != null) {
          bitSetStore = BitSet.valueOf(original.getBytesStore());
        } else {
          bitSetStore =
              ValueRanges.convertFromRangesToBitSet(original.getRangesList());
        }
      }
    }

    result.setBitSetStore(bitSetStore);
    return result;
  }

  public static BitSet convertFromRangesToBitSet(List<ValueRange> rangesList) {
    BitSet bitSetStore = new BitSet();

    if (rangesList != null) {
      for (ValueRange range : rangesList) {
        int start = range.getBegin();
        int end = range.getEnd();
        bitSetStore.set(start, end + 1);
      }
    }
    return bitSetStore;
  }

  public static List<ValueRange> convertFromBitSetToRanges(BitSet bitSetStore) {
    List<ValueRange> resultList = new ArrayList<ValueRange>();

    if (bitSetStore != null) {
      for (int start = bitSetStore.nextSetBit(0); start >= 0;) {
        int end = bitSetStore.nextClearBit(start) - 1;
        ValueRange range = ValueRange.newInstance(start, end);
        resultList.add(range);
        start = bitSetStore.nextSetBit(end + 1);
      }
    }
    return resultList;
  }

  public boolean isLessOrEqual(ValueRanges other) {
    if (other == null) {
      return false;
    }

    BitSet leftBitSetStore = this.getBitSetStore();
    BitSet rightBitSetStore = other.getBitSetStore();
    boolean leftBitSetStored = (this.getBitSetStore() != null);
    boolean rightBitSetStored = (other.getBitSetStore() != null);

    if (leftBitSetStored && rightBitSetStored) {
      if (leftBitSetStore.length() > rightBitSetStore.length()) {
        return false;
      }
      for (int i = 0; i < leftBitSetStore.length(); i++) {
        if (leftBitSetStore.get(i) && !rightBitSetStore.get(i)) {
          return false;
        }
      }
      return true;
    } else if (leftBitSetStored && !rightBitSetStored) {
      for (ValueRange rightRange : coalesce(other).getRangesList()) {
        leftBitSetStore.clear(rightRange.getBegin(), rightRange.getEnd() + 1);
      }
      return leftBitSetStore.cardinality() == 0;
    } else if (!leftBitSetStored && rightBitSetStored) {
      for (ValueRange leftRange : coalesce(this).getRangesList()) {
        for (int i = leftRange.getBegin(); i <= leftRange.getEnd(); i++) {
          if (!rightBitSetStore.get(i)) {
            return false;
          }
        }
      }
      return true;
    } else {
      ValueRanges left = coalesce(this);
      ValueRanges right = coalesce(other);
      for (ValueRange leftRange : left.getRangesList()) {
        boolean matched = false;
        for (ValueRange rightRange : right.getRangesList()) {
          if (leftRange.isLessOrEqual(rightRange)) {
            matched = true;
            break;
          }
        }
        if (!matched) {
          return false;
        }
      }
      return true;
    }
  }

  public static ValueRanges add(ValueRanges left, ValueRanges right) {
    if (left == null) {
      return coalesce(right);
    }
    if (right == null) {
      return coalesce(left);
    }
    return coalesce(left, right);
  }

  public static ValueRanges minus(ValueRanges left, ValueRanges right) {
    if (left == null) {
      return null;
    }
    if (right == null) {
      return coalesce(left);
    }
    return coalesce(left).minusSelf(right);
  }

  public ValueRanges addSelf(ValueRanges other) {
    if (other == null) {
      return coalesce(this);
    }
    return coalesce(this, other);
  }

  public ValueRanges minusSelf(ValueRanges other) {
    if (other == null) {
      return this;
    }

    BitSet leftBitSetStore = this.getBitSetStore();
    BitSet rightBitSetStore = other.getBitSetStore();
    boolean leftBitSetStored = (this.getBitSetStore() != null);
    boolean rightBitSetStored = (other.getBitSetStore() != null);

    ValueRanges result = ValueRanges.newInstance();

    if (leftBitSetStored && rightBitSetStored) {
      leftBitSetStore.andNot(rightBitSetStore);

      result.setBitSetStore(leftBitSetStore);
      // to return ValueRanges which has the same store style to left
    } else if (leftBitSetStored && !rightBitSetStored) {
      for (ValueRange rightRange : coalesce(other).getRangesList()) {
        leftBitSetStore.set(rightRange.getBegin(), rightRange.getEnd() + 1,
            false);
      }

      result.setBitSetStore(leftBitSetStore);
    } else if (!leftBitSetStored && rightBitSetStored) {
      BitSet bitSetStore = new BitSet();
      for (ValueRange leftRange : coalesce(this).getRangesList()) {
        bitSetStore.set(leftRange.getBegin(), leftRange.getEnd() + 1, true);
      }
      bitSetStore.andNot(rightBitSetStore);
      List<ValueRange> resultList = convertFromBitSetToRanges(bitSetStore);

      result.setRangesList(resultList);
      result.setCoalesced(true);
    } else {
      List<ValueRange> leftList = cloneList(coalesce(this).getRangesList());
      List<ValueRange> rightList = coalesce(other).getRangesList();
      int i = 0;
      int j = 0;
      while (i < leftList.size() && j < rightList.size()) {
        ValueRange left = leftList.get(i);
        ValueRange right = rightList.get(j);
        // 1. no overlap, right is bigger than left
        if (left.getEnd() < right.getBegin()) {
          i++;
          // 2. no overlap, left is bigger than right
        } else if (right.getEnd() < left.getBegin()) {
          j++;
          // 3. has overlap, left is less than right
        } else if ((left.getBegin() <= right.getBegin())
            && (left.getEnd() <= right.getEnd())) {
          if (left.getBegin() == right.getBegin()) {
            leftList.remove(i);
          } else {
            left.setEnd(right.getBegin() - 1);
          }
          // 4. has overlap, left is bigger than right
        } else if ((left.getBegin() >= right.getBegin())
            && (left.getEnd() >= right.getEnd())) {
          if (left.getEnd() == right.getEnd()) {
            leftList.remove(i);
          } else {
            left.setBegin(right.getEnd() + 1);
          }
          // 5. left contains right
        } else if ((left.getBegin() < right.getBegin())
            && (left.getEnd() > right.getEnd())) {
          ValueRange newRange =
              ValueRange.newInstance(right.getEnd() + 1, left.getEnd());
          leftList.add(i + 1, newRange);
          left.setEnd(right.getBegin() - 1);
          // 6. right contains left
        } else if ((left.getBegin() > right.getBegin())
            && (left.getEnd() < right.getEnd())) {
          leftList.remove(i);
        }
      }

      result.setRangesList(leftList);
      result.setCoalesced(true);
    }
    return result;
  }

  /**
   * Coalescing ValueRanges
   * 
   * @param left, may be ValueRanges or BitSetStores
   * @param right, may be ValueRanges or BitSetStores
   * @return merged ValueRanges whose internal store type is the same as left
   */
  private static ValueRanges coalesce(ValueRanges left, ValueRanges right) {
    if (left == null) {
      return right;
    }
    if (right == null) {
      return left;
    }

    BitSet leftBitSetStore = left.getBitSetStore();
    BitSet rightBitSetStore = right.getBitSetStore();
    boolean leftBitSetStored = (left.getBitSetStore() != null);
    boolean rightBitSetStored = (right.getBitSetStore() != null);

    ValueRanges mergedRanges = ValueRanges.newInstance();
    if (leftBitSetStored && rightBitSetStored) {
      BitSet bitSetStores = new BitSet();
      bitSetStores.or(leftBitSetStore);
      bitSetStores.or(rightBitSetStore);

      mergedRanges.setBitSetStore(bitSetStores);

    } else if (leftBitSetStored && !rightBitSetStored) {
      for (ValueRange rightRange : right.getRangesList()) {
        leftBitSetStore.set(rightRange.getBegin(), rightRange.getEnd() + 1,
            true);
      }

      mergedRanges.setBitSetStore(leftBitSetStore);
    } else if (!leftBitSetStored && rightBitSetStored) {
      List<ValueRange> rangesList = cloneList(left.getSortedRangesList());
      rangesList.addAll(convertFromBitSetToRanges(rightBitSetStore));
      Collections.sort(rangesList);

      mergedRanges.setRangesList(coalesceList(rangesList));
      mergedRanges.setCoalesced(true);
    } else {
      List<ValueRange> leftList = cloneList(left.getRangesList());
      leftList.addAll(cloneList(right.getRangesList()));
      Collections.sort(leftList);

      mergedRanges.setRangesList(coalesceList(leftList));
      mergedRanges.setCoalesced(true);
    }
    return mergedRanges;
  }

  private static List<ValueRange> coalesceList(List<ValueRange> sortedList) {
    if (sortedList == null || sortedList.isEmpty()) {
      return sortedList;
    }

    List<ValueRange> resultList = new ArrayList<ValueRange>();

    ValueRange current = sortedList.get(0).clone();
    resultList.add(current);

    // In a single pass, we compute the size of the end result, as well as
    // modify
    // in place the intermediate data structure to build up result as we
    // solve it.

    for (ValueRange range : sortedList) {
      // Skip if this range is equivalent to the current range.
      if (range.getBegin() == current.getBegin()
          && range.getEnd() == current.getEnd()) {
        continue;
      }
      // If the current range just needs to be extended on the right.
      if (range.getBegin() == current.getBegin()
          && range.getEnd() > current.getEnd()) {
        current.setEnd(range.getEnd());
      } else if (range.getBegin() > current.getBegin()) {
        // If we are starting farther ahead, then there are 2 cases:
        if (range.getBegin() <= current.getEnd() + 1) {
          // 1. Ranges are overlapping and we can merge them.
          current.setEnd(Math.max(current.getEnd(), range.getEnd()));
        } else {
          // 2. No overlap and we are adding a new range.
          current = range.clone();
          resultList.add(current);
        }
      }
    }
    return resultList;
  }

  /**
   *
   * @param uranges that may be ValueRanges or BitSetStores, if it's
   *          BitSetStores, do nothing
   * @return ValueRanges that is coalesced
   */
  private static ValueRanges coalesce(ValueRanges uranges) {
    if (uranges == null) {
      return null;
    }

    if (uranges.isCoalesced()) {
      return uranges;
    }

    if (uranges.getBitSetStore() != null) {
      return uranges;
    }

    ValueRanges result = ValueRanges.newInstance();
    if (uranges.getRangesCount() == 0) {
      return result;
    }
    List<ValueRange> rangesList = uranges.getSortedRangesList();

    result.setRangesList(coalesceList(rangesList));
    result.setCoalesced(true);

    return result;
  }

  public synchronized static List<ValueRange> cloneList(List<ValueRange> list) {
    List<ValueRange> newList = new ArrayList<ValueRange>();
    for (ValueRange range : list) {
      newList.add(range.clone());
    }
    return newList;
  }

  public abstract int getRangesCount();

  /**
   * This method is used to check if the ValueRanges coalesced, coalesced means
   * no override parts and well sorted. For example, [1-3],[5-10] is coalesced,
   * and [1-4],[3-10] and [5-10].[1-3] is not.
   * 
   * @return true or false
   */
  public abstract boolean isCoalesced();

  public abstract void setCoalesced(boolean flag);

  /**
   * Initialize the ValueRanges from expression, we current support[1-3],[5-10]
   * style
   * 
   * @param expression
   * @return
   */
  public static ValueRanges iniFromExpression(String expression) {
    return iniFromExpression(expression, false);
  }

  /**
   * Initialize the ValueRanges from expression, we currently
   * support[1-3],[5-10] style
   *
   * @param expression
   * @return ValueRanges
   */
  public static ValueRanges iniFromExpression(String expression,
      boolean enableBitSet) {
    ValueRanges valueRanges = Records.newRecord(ValueRanges.class);
    String[] items = expression.split(",");
    Pattern pattern = Pattern.compile("^\\[(\\d+)\\-(\\d+)\\]$");
    // Generate rangeList or bitSetStore
    List<ValueRange> rangesList = new ArrayList<ValueRange>();
    BitSet bitSetStore = new BitSet();

    for (String item : items) {
      Matcher matcher = pattern.matcher(item);
      if (matcher.find()) {
        int start = Integer.parseInt(matcher.group(1));
        int end = Integer.parseInt(matcher.group(2));
        if (enableBitSet) {
          bitSetStore.set(start, end + 1);
        } else {
          rangesList.add(ValueRange.newInstance(start, end));
        }
      } else {
        try {
          int num = Integer.parseInt(item);
          if (enableBitSet) {
            bitSetStore.set(num);
          } else {
            rangesList.add(ValueRange.newInstance(num, num));
          }
        } catch (NumberFormatException e) {
          // ignore this num
        }
      }
    }
    if (enableBitSet) {
      valueRanges.setBitSetStore(bitSetStore);
      valueRanges.setByteStoreEnable(true);
    } else {
      valueRanges.setRangesList(rangesList);
    }
    return valueRanges;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof ValueRanges))
      return false;
    ValueRanges other = (ValueRanges) obj;
    if (this.equals(other)) {
      return true;
    } else {
      return false;
    }
  }

  public synchronized boolean equals(ValueRanges other) {
    if (other == null) {
      return false;
    }

    BitSet leftBitSetStore = this.getBitSetStore();
    BitSet rightBitSetStore = other.getBitSetStore();
    boolean leftBitSetStored = (this.getBitSetStore() != null);
    boolean rightBitSetStored = (other.getBitSetStore() != null);

    if (leftBitSetStored && rightBitSetStored) {
      return leftBitSetStore.equals(rightBitSetStore);
    } else if (leftBitSetStored || rightBitSetStored) {
      ValueRanges valueRanges =
          leftBitSetStored ? coalesce(other) : coalesce(this);
      BitSet bitSetStore =
          leftBitSetStored ? leftBitSetStore : rightBitSetStore;
      int count = 0;
      for (ValueRange range : valueRanges.getRangesList()) {
        for (int i = range.getBegin(); i <= range.getEnd(); i++) {
          if (!bitSetStore.get(i)) {
            return false;
          }
        }
        count += range.getEnd() - range.getBegin() + 1;
      }
      return count == bitSetStore.cardinality();
    } else {
      ValueRanges left = coalesce(this);
      ValueRanges right = coalesce(other);
      if (left.getRangesCount() != right.getRangesCount()) {
        return false;
      }
      List<ValueRange> leftRange = left.getRangesList();
      List<ValueRange> rightRange = right.getRangesList();
      for (int i = 0; i < left.getRangesCount(); i++) {
        if (!leftRange.get(i).equals(rightRange.get(i))) {
          return false;
        }
      }
      return true;
    }
  }

  @Override
  public int hashCode() {
    return getRangesList().hashCode();
  }

  @Override
  public int compareTo(ValueRanges other) {
    if (this.equals(other)) {
      return 0;
    } else if (this.isLessOrEqual(other)) {
      return -1;
    } else {
      return 1;
    }
  }
}

