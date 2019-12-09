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
package org.apache.hadoop.tools.rumen.anonymization;


import org.apache.commons.lang3.StringUtils;

/**
 * Utility class to handle commonly performed tasks in a 
 * {@link org.apache.hadoop.tools.rumen.datatypes.DefaultAnonymizableDataType} 
 * using a {@link WordList} for anonymization.
 * //TODO There is no caching for saving memory.
 */
public class WordListAnonymizerUtility {
  static final String[] KNOWN_WORDS = 
    new String[] {"job", "tmp", "temp", "home", "homes", "usr", "user", "test"};
  
  /**
   * Checks if the data needs anonymization. Typically, data types which are 
   * numeric in nature doesn't need anonymization.
   */
  public static boolean needsAnonymization(String data) {
    // Numeric data doesn't need anonymization
    // Currently this doesnt support inputs like
    //   - 12.3
    //   - 12.3f
    //   - 90L
    //   - 1D
    if (StringUtils.isNumeric(data)) {
      return false;
    }
    return true; // by default return true
  }
  
  /**
   * Checks if the given data has a known suffix.
   */
  public static boolean hasSuffix(String data, String[] suffixes) {
    // check if they end in known suffixes
    for (String ks : suffixes) {
      if (data.endsWith(ks)) {
        return true;
      }
    }
    
    return false;
  }
  
  /**
   * Extracts a known suffix from the given data.
   * 
   * @throws RuntimeException if the data doesn't have a suffix. 
   *         Use {@link #hasSuffix(String, String[])} to make sure that the 
   *         given data has a suffix.
   */
  public static String[] extractSuffix(String data, String[] suffixes) {
    // check if they end in known suffixes
    String suffix = "";
    for (String ks : suffixes) {
      if (data.endsWith(ks)) {
        suffix = ks;
        // stripe off the suffix which will get appended later
        data = data.substring(0, data.length() - suffix.length());
        return new String[] {data, suffix};
      }
    }
    
    // throw exception
    throw new RuntimeException("Data [" + data + "] doesn't have a suffix from" 
        + " known suffixes [" + StringUtils.join(suffixes, ',') + "]");
  }
  
  /**
   * Checks if the given data is known. This API uses {@link #KNOWN_WORDS} to
   * detect if the given data is a commonly used (so called 'known') word.
   */
  public static boolean isKnownData(String data) {
    return isKnownData(data, KNOWN_WORDS);
  }
  
  /**
   * Checks if the given data is known.
   */
  public static boolean isKnownData(String data, String[] knownWords) {
    // check if the data is known content
    //TODO [Chunking] Do this for sub-strings of data
    
    for (String kd : knownWords) {
      if (data.equals(kd)) {
        return true;
      }
    }
    return false;
  }
}