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
package org.apache.hadoop.mapred.gridmix;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.mapred.gridmix.RandomTextDataGenerator;

import static org.junit.Assert.*;
import org.junit.Test;

/**
 * Test {@link RandomTextDataGenerator}.
 */
public class TestRandomTextDataGenerator {
  /**
   * Test if {@link RandomTextDataGenerator} can generate random words of 
   * desired size.
   */
  @Test
  public void testRandomTextDataGenerator() {
    RandomTextDataGenerator rtdg = new RandomTextDataGenerator(10, 0L, 5);
    List<String> words = rtdg.getRandomWords();

    // check the size
    assertEquals("List size mismatch", 10, words.size());

    // check the words
    Set<String> wordsSet = new HashSet<String>(words);
    assertEquals("List size mismatch due to duplicates", 10, wordsSet.size());

    // check the word lengths
    for (String word : wordsSet) {
      assertEquals("Word size mismatch", 5, word.length());
    }
  }
  
  /**
   * Test if {@link RandomTextDataGenerator} can generate same words given the
   * same list-size, word-length and seed.
   */
  @Test
  public void testRandomTextDataGeneratorRepeatability() {
    RandomTextDataGenerator rtdg1 = new RandomTextDataGenerator(10, 0L, 5);
    List<String> words1 = rtdg1.getRandomWords();

    RandomTextDataGenerator rtdg2 = new RandomTextDataGenerator(10, 0L, 5);
    List<String> words2 = rtdg2.getRandomWords();
    
    assertTrue("List mismatch", words1.equals(words2));
  }
  
  /**
   * Test if {@link RandomTextDataGenerator} can generate different words given 
   * different seeds.
   */
  @Test
  public void testRandomTextDataGeneratorUniqueness() {
    RandomTextDataGenerator rtdg1 = new RandomTextDataGenerator(10, 1L, 5);
    Set<String> words1 = new HashSet(rtdg1.getRandomWords());

    RandomTextDataGenerator rtdg2 = new RandomTextDataGenerator(10, 0L, 5);
    Set<String> words2 = new HashSet(rtdg2.getRandomWords());
    
    assertFalse("List size mismatch across lists", words1.equals(words2));
  }
}
