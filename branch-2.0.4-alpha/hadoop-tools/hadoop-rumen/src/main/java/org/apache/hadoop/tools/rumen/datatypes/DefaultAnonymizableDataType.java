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
package org.apache.hadoop.tools.rumen.datatypes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.rumen.anonymization.WordList;
import org.apache.hadoop.tools.rumen.anonymization.WordListAnonymizerUtility;
import org.apache.hadoop.tools.rumen.state.StatePool;

/**
 * Represents a default anonymizable Rumen data-type. It uses 
 * {@link WordListAnonymizerUtility} for anonymization.
 */
public abstract class DefaultAnonymizableDataType 
implements AnonymizableDataType<String> {
  private static final String DEFAULT_PREFIX = "data";
  
  protected String getPrefix() {
    return DEFAULT_PREFIX;
  }
  
  // Determines if the contained data needs anonymization
  protected boolean needsAnonymization(Configuration conf) {
    return true;
  }
  
  @Override
  public final String getAnonymizedValue(StatePool statePool, 
                                         Configuration conf) {
    if (needsAnonymization(conf)) {
      WordList state = (WordList) statePool.getState(getClass());
      if (state == null) {
        state = new WordList(getPrefix());
        statePool.addState(getClass(), state);
      }
      return anonymize(getValue(), state);
    } else {
      return getValue();
    }
  }
  
  private static String anonymize(String data, WordList wordList) {
    if (data == null) {
      return null;
    }

    if (!wordList.contains(data)) {
      wordList.add(data);
    }
    return wordList.getName() + wordList.indexOf(data);
  }
}