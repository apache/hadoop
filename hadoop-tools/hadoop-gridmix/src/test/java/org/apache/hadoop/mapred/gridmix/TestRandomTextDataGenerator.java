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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

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
    assertEquals(10, words.size(), "List size mismatch");

    // check the words
    Set<String> wordsSet = new HashSet<String>(words);
    assertEquals(10, wordsSet.size(), "List size mismatch due to duplicates");

    // check the word lengths
    for (String word : wordsSet) {
      assertEquals(5, word.length(), "Word size mismatch");
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
    
    assertTrue(words1.equals(words2), "List mismatch");
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
    
    assertFalse(words1.equals(words2), "List size mismatch across lists");
  }
}
