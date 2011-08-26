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

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * A random text generator. The words are simply sequences of alphabets.
 */
class RandomTextDataGenerator {
  static final Log LOG = LogFactory.getLog(RandomTextDataGenerator.class);
  
  /**
   * Configuration key for random text data generator's list size.
   */
  static final String GRIDMIX_DATAGEN_RANDOMTEXT_LISTSIZE = 
    "gridmix.datagenerator.randomtext.listsize";
  
  /**
   * Configuration key for random text data generator's word size.
   */
  static final String GRIDMIX_DATAGEN_RANDOMTEXT_WORDSIZE = 
    "gridmix.datagenerator.randomtext.wordsize";
  
  /**
   * Default random text data generator's list size.
   */
  static final int DEFAULT_LIST_SIZE = 200;
  
  /**
   * Default random text data generator's word size.
   */
  static final int DEFAULT_WORD_SIZE = 10;
  
  /**
   * Default random text data generator's seed.
   */
  static final long DEFAULT_SEED = 0L;
  
  /**
   * A list of random words
   */
  private String[] words;
  private Random random;
  
  /**
   * Constructor for {@link RandomTextDataGenerator} with default seed.
   * @param size the total number of words to consider.
   * @param wordSize Size of each word
   */
  RandomTextDataGenerator(int size, int wordSize) {
    this(size, DEFAULT_SEED , wordSize);
  }
  
  /**
   * Constructor for {@link RandomTextDataGenerator}.
   * @param size the total number of words to consider.
   * @param seed Random number generator seed for repeatability
   * @param wordSize Size of each word
   */
  RandomTextDataGenerator(int size, Long seed, int wordSize) {
    random = new Random(seed);
    words = new String[size];
    
    //TODO change the default with the actual stats
    //TODO do u need varied sized words?
    for (int i = 0; i < size; ++i) {
      words[i] = 
        RandomStringUtils.random(wordSize, 0, 0, true, false, null, random);
    }
  }
  
  /**
   * Get the configured random text data generator's list size.
   */
  static int getRandomTextDataGeneratorListSize(Configuration conf) {
    return conf.getInt(GRIDMIX_DATAGEN_RANDOMTEXT_LISTSIZE, DEFAULT_LIST_SIZE);
  }
  
  /**
   * Set the random text data generator's list size.
   */
  static void setRandomTextDataGeneratorListSize(Configuration conf, 
                                                 int listSize) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Random text data generator is configured to use a dictionary " 
                + " with " + listSize + " words");
    }
    conf.setInt(GRIDMIX_DATAGEN_RANDOMTEXT_LISTSIZE, listSize);
  }
  
  /**
   * Get the configured random text data generator word size.
   */
  static int getRandomTextDataGeneratorWordSize(Configuration conf) {
    return conf.getInt(GRIDMIX_DATAGEN_RANDOMTEXT_WORDSIZE, DEFAULT_WORD_SIZE);
  }
  
  /**
   * Set the random text data generator word size.
   */
  static void setRandomTextDataGeneratorWordSize(Configuration conf, 
                                                 int wordSize) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Random text data generator is configured to use a dictionary " 
                + " with words of length " + wordSize);
    }
    conf.setInt(GRIDMIX_DATAGEN_RANDOMTEXT_WORDSIZE, wordSize);
  }
  
  /**
   * Returns a randomly selected word from a list of random words.
   */
  String getRandomWord() {
    int index = random.nextInt(words.length);
    return words[index];
  }
  
  /**
   * This is mainly for testing.
   */
  List<String> getRandomWords() {
    return Arrays.asList(words);
  }
}
