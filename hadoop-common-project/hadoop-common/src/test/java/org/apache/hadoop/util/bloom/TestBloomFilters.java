package org.apache.hadoop.util.bloom;

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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.AbstractCollection;
import java.util.BitSet;
import java.util.Iterator;

import org.apache.hadoop.util.bloom.BloomFilterCommonTester.BloomFilterTestStrategy;
import org.apache.hadoop.util.hash.Hash;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class TestBloomFilters {

  int numInsertions = 1000;
  int bitSize = BloomFilterCommonTester.optimalNumOfBits(numInsertions, 0.03);
  int hashFunctionNumber = 5;

  private static final ImmutableMap<Integer, ? extends AbstractCollection<Key>> FALSE_POSITIVE_UNDER_1000 = ImmutableMap
      .of(Hash.JENKINS_HASH, new AbstractCollection<Key>() {
        final ImmutableList<Key> falsePositive = ImmutableList.<Key> of(
            new Key("99".getBytes()), new Key("963".getBytes()));

        @Override
        public Iterator<Key> iterator() {
          return falsePositive.iterator();
        }

        @Override
        public int size() {
          return falsePositive.size();
        }
      }, Hash.MURMUR_HASH, new AbstractCollection<Key>() {
        final ImmutableList<Key> falsePositive = ImmutableList.<Key> of(
            new Key("769".getBytes()), new Key("772".getBytes()),
            new Key("810".getBytes()), new Key("874".getBytes()));

        @Override
        public Iterator<Key> iterator() {
          return falsePositive.iterator();
        }

        @Override
        public int size() {
          return falsePositive.size();
        }
      });

  private enum Digits {
    ODD(1), EVEN(0);

    int start;

    Digits(int start) {
      this.start = start;
    }

    int getStart() {
      return start;
    }
  }
  
  @Test
  public void testDynamicBloomFilter() {
    int hashId = Hash.JENKINS_HASH;    
    Filter filter = new DynamicBloomFilter(bitSize, hashFunctionNumber,
        Hash.JENKINS_HASH, 3);    
    BloomFilterCommonTester.of(hashId, numInsertions)
        .withFilterInstance(filter)
        .withTestCases(ImmutableSet.of(BloomFilterTestStrategy.KEY_TEST_STRATEGY,
                BloomFilterTestStrategy.ADD_KEYS_STRATEGY,
                BloomFilterTestStrategy.EXCEPTIONS_CHECK_STRATEGY,
                BloomFilterTestStrategy.WRITE_READ_STRATEGY,
                BloomFilterTestStrategy.ODD_EVEN_ABSENT_STRATEGY))
                .test();
    
    assertNotNull("testDynamicBloomFilter error ", filter.toString());
  }

  @Test
  public void testCountingBloomFilter() {
    int hashId = Hash.JENKINS_HASH;

    CountingBloomFilter filter = new CountingBloomFilter(bitSize,
        hashFunctionNumber, hashId);

    Key key = new Key(new byte[] { 48, 48 });

    filter.add(key);
    assertTrue("CountingBloomFilter.membership error ",
        filter.membershipTest(key));
    assertTrue("CountingBloomFilter.approximateCount error",
        filter.approximateCount(key) == 1);

    filter.add(key);
    assertTrue("CountingBloomFilter.approximateCount error",
        filter.approximateCount(key) == 2);

    filter.delete(key);
    assertTrue("CountingBloomFilter.membership error ",
        filter.membershipTest(key));

    filter.delete(key);
    assertFalse("CountingBloomFilter.membership error ",
        filter.membershipTest(key));
    assertTrue("CountingBloomFilter.approximateCount error",
        filter.approximateCount(key) == 0);

    BloomFilterCommonTester.of(hashId, numInsertions)
        .withFilterInstance(filter)
        .withTestCases(ImmutableSet.of(BloomFilterTestStrategy.KEY_TEST_STRATEGY,
                BloomFilterTestStrategy.ADD_KEYS_STRATEGY,
                BloomFilterTestStrategy.EXCEPTIONS_CHECK_STRATEGY,
                BloomFilterTestStrategy.ODD_EVEN_ABSENT_STRATEGY,
                BloomFilterTestStrategy.WRITE_READ_STRATEGY,
                BloomFilterTestStrategy.FILTER_OR_STRATEGY,
                BloomFilterTestStrategy.FILTER_XOR_STRATEGY)).test();
  }

  @Test
  public void testRetouchedBloomFilterSpecific() {
    int numInsertions = 1000;
    int hashFunctionNumber = 5;

    ImmutableSet<Integer> hashes = ImmutableSet.of(Hash.MURMUR_HASH,
        Hash.JENKINS_HASH);

    for (Integer hashId : hashes) {      
      RetouchedBloomFilter filter = new RetouchedBloomFilter(bitSize,
          hashFunctionNumber, hashId);

      checkOnAbsentFalsePositive(hashId, numInsertions, filter, Digits.ODD,
          RemoveScheme.MAXIMUM_FP);
      filter.and(new RetouchedBloomFilter(bitSize, hashFunctionNumber, hashId));

      checkOnAbsentFalsePositive(hashId, numInsertions, filter, Digits.EVEN,
          RemoveScheme.MAXIMUM_FP);
      filter.and(new RetouchedBloomFilter(bitSize, hashFunctionNumber, hashId));

      checkOnAbsentFalsePositive(hashId, numInsertions, filter, Digits.ODD,
          RemoveScheme.MINIMUM_FN);
      filter.and(new RetouchedBloomFilter(bitSize, hashFunctionNumber, hashId));

      checkOnAbsentFalsePositive(hashId, numInsertions, filter, Digits.EVEN,
          RemoveScheme.MINIMUM_FN);
      filter.and(new RetouchedBloomFilter(bitSize, hashFunctionNumber, hashId));

      checkOnAbsentFalsePositive(hashId, numInsertions, filter, Digits.ODD,
          RemoveScheme.RATIO);
      filter.and(new RetouchedBloomFilter(bitSize, hashFunctionNumber, hashId));

      checkOnAbsentFalsePositive(hashId, numInsertions, filter, Digits.EVEN,
          RemoveScheme.RATIO);
      filter.and(new RetouchedBloomFilter(bitSize, hashFunctionNumber, hashId));
    }
  }

  private void checkOnAbsentFalsePositive(int hashId, int numInsertions,
      final RetouchedBloomFilter filter, Digits digits, short removeSchema) {
    AbstractCollection<Key> falsePositives = FALSE_POSITIVE_UNDER_1000
        .get(hashId);

    if (falsePositives == null)
      Assert.fail(String.format("false positives for hash %d not founded",
          hashId));

    filter.addFalsePositive(falsePositives);

    for (int i = digits.getStart(); i < numInsertions; i += 2) {
      filter.add(new Key(Integer.toString(i).getBytes()));
    }

    for (Key key : falsePositives) {
      filter.selectiveClearing(key, removeSchema);
    }

    for (int i = 1 - digits.getStart(); i < numInsertions; i += 2) {
      assertFalse(" testRetouchedBloomFilterAddFalsePositive error " + i,
          filter.membershipTest(new Key(Integer.toString(i).getBytes())));
    }
  }

  @Test
  public void testFiltersWithJenkinsHash() {
    int hashId = Hash.JENKINS_HASH;

    BloomFilterCommonTester.of(hashId, numInsertions)
        .withFilterInstance(new BloomFilter(bitSize, hashFunctionNumber, hashId))
        .withFilterInstance(new RetouchedBloomFilter(bitSize, hashFunctionNumber, hashId))
        .withTestCases(ImmutableSet.of(BloomFilterTestStrategy.KEY_TEST_STRATEGY,
                BloomFilterTestStrategy.ADD_KEYS_STRATEGY,
                BloomFilterTestStrategy.EXCEPTIONS_CHECK_STRATEGY,
                BloomFilterTestStrategy.ODD_EVEN_ABSENT_STRATEGY,
                BloomFilterTestStrategy.WRITE_READ_STRATEGY,
                BloomFilterTestStrategy.FILTER_OR_STRATEGY,
                BloomFilterTestStrategy.FILTER_AND_STRATEGY,
                BloomFilterTestStrategy.FILTER_XOR_STRATEGY)).test();
  }

  @Test
  public void testFiltersWithMurmurHash() {
    int hashId = Hash.MURMUR_HASH;

    BloomFilterCommonTester.of(hashId, numInsertions)
        .withFilterInstance(new BloomFilter(bitSize, hashFunctionNumber, hashId))
        .withFilterInstance(new RetouchedBloomFilter(bitSize, hashFunctionNumber, hashId))
        .withTestCases(ImmutableSet.of(BloomFilterTestStrategy.KEY_TEST_STRATEGY,
                BloomFilterTestStrategy.ADD_KEYS_STRATEGY,
                BloomFilterTestStrategy.EXCEPTIONS_CHECK_STRATEGY,
                BloomFilterTestStrategy.ODD_EVEN_ABSENT_STRATEGY,
                BloomFilterTestStrategy.WRITE_READ_STRATEGY,
                BloomFilterTestStrategy.FILTER_OR_STRATEGY,
                BloomFilterTestStrategy.FILTER_AND_STRATEGY,
                BloomFilterTestStrategy.FILTER_XOR_STRATEGY)).test();
  }

  @Test
  public void testFiltersWithLargeVectorSize() {
    int hashId = Hash.MURMUR_HASH;
    Filter filter
        = new BloomFilter(Integer.MAX_VALUE, hashFunctionNumber, hashId);
    BloomFilterCommonTester.of(hashId, numInsertions)
        .withFilterInstance(filter)
        .withTestCases(ImmutableSet.of(
                BloomFilterTestStrategy.WRITE_READ_STRATEGY
        )).test();
  }

  @Test
  public void testNot() {
    BloomFilter bf = new BloomFilter(8, 1, Hash.JENKINS_HASH);
    bf.bits = BitSet.valueOf(new byte[] { (byte) 0x95 });
    BitSet origBitSet = (BitSet) bf.bits.clone();
    bf.not();
    assertFalse("BloomFilter#not should have inverted all bits",
                bf.bits.intersects(origBitSet));
  }
}
