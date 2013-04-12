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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.tools.rumen.state.State;

/**
 * Represents the list of words used in list-backed anonymizers.
 */
public class WordList implements State {
  private Map<String, Integer> list = new HashMap<String, Integer>(0);
  private boolean isUpdated = false;
  private String name;

  public WordList() {
    this("word");
  }

  public WordList(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  /**
   * Adds the specified word to the list if the word is not already added.
   */
  public void add(String word) {
    if (!contains(word)) {
      int index = getSize();
      list.put(word, index);
      isUpdated = true;
    }
  }

  /**
   * Returns 'true' if the list contains the specified word.
   */
  public boolean contains(String word) {
    return list.containsKey(word);
  }

  /**
   * Returns the index of the specified word in the list.
   */
  public int indexOf(String word) {
    return list.get(word);
  }

  /**
   * Returns the size of the list.
   */
  public int getSize() {
    return list.size();
  }

  /**
   * Returns 'true' if the list is updated since creation (and reload).
   */
  @Override
  public boolean isUpdated() {
    return isUpdated;
  }
  
  /**
   * Setters and getters for Jackson JSON
   */
  /**
   * Sets the size of the list.
   * 
   * Note: That this API is only for Jackson JSON deserialization.
   */
  public void setSize(int size) {
    list = new HashMap<String, Integer>(size);
  }
  
  /**
   * Note: That this API is only for Jackson JSON deserialization.
   */
  @Override
  public void setName(String name) {
    this.name = name;
  }
  
  /**
   * Gets the words.
   * 
   * Note: That this API is only for Jackson JSON serialization.
   */
  public Map<String, Integer> getWords() {
    return list;
  }
  
  /**
   * Sets the words. 
   * 
   * Note: That this API is only for Jackson JSON deserialization.
   */
  public void setWords(Map<String, Integer> list) {
    this.list = list;
  }
}
