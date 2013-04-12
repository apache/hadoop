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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.rumen.anonymization.WordList;
import org.apache.hadoop.tools.rumen.anonymization.WordListAnonymizerUtility;
import org.apache.hadoop.tools.rumen.state.State;
import org.apache.hadoop.tools.rumen.state.StatePool;
import org.apache.hadoop.util.StringUtils;

/**
 * Represents a file's location.
 * 
 * Currently, only filenames that can be represented using {@link Path} are 
 * supported.
 */
public class FileName implements AnonymizableDataType<String> {
  private final String fileName;
  private String anonymizedFileName;
  private static final String PREV_DIR = "..";
  private static final String[] KNOWN_SUFFIXES = 
    new String[] {".xml", ".jar", ".txt", ".tar", ".zip", ".json", ".gzip", 
                  ".lzo"};
  
  /**
   * A composite state for filename.
   */
  public static class FileNameState implements State {
    private WordList dirState = new WordList("dir");
    private WordList fileNameState =  new WordList("file");
    
    @Override
    public boolean isUpdated() {
      return dirState.isUpdated() || fileNameState.isUpdated();
    }
    
    public WordList getDirectoryState() {
      return dirState;
    }
    
    public WordList getFileNameState() {
      return fileNameState;
    }
    
    public void setDirectoryState(WordList state) {
      this.dirState = state;
    }
    
    public void setFileNameState(WordList state) {
      this.fileNameState = state;
    }
    
    @Override
    public String getName() {
      return "path";
    }
    
    @Override
    public void setName(String name) {
      // for now, simply assert since this class has a hardcoded name
      if (!getName().equals(name)) {
        throw new RuntimeException("State name mismatch! Expected '" 
                                   + getName() + "' but found '" + name + "'.");
      }
    }
  }
  
  public FileName(String fileName) {
    this.fileName = fileName;
  }
  
  @Override
  public String getValue() {
    return fileName;
  }
  
  @Override
  public String getAnonymizedValue(StatePool statePool, 
                                   Configuration conf) {
    if (anonymizedFileName == null) {
      anonymize(statePool, conf);
    }
    return anonymizedFileName;
  }
  
  private void anonymize(StatePool statePool, Configuration conf) {
    FileNameState fState = (FileNameState) statePool.getState(getClass());
    if (fState == null) {
      fState = new FileNameState();
      statePool.addState(getClass(), fState);
    }
    
    String[] files = StringUtils.split(fileName);
    String[] anonymizedFileNames = new String[files.length];
    int i = 0;
    for (String f : files) {
      anonymizedFileNames[i++] = 
        anonymize(statePool, conf, fState, f);
    }

    anonymizedFileName = StringUtils.arrayToString(anonymizedFileNames);
  }
  
  private static String anonymize(StatePool statePool, Configuration conf, 
                                  FileNameState fState, String fileName) {
    String ret = null;
    try {
      URI uri = new URI(fileName);
      
      // anonymize the path i.e without the authority & scheme
      ret = 
        anonymizePath(uri.getPath(), fState.getDirectoryState(), 
                      fState.getFileNameState());
      
      // anonymize the authority and scheme
      String authority = uri.getAuthority();
      String scheme = uri.getScheme();
      if (scheme != null) {
        String anonymizedAuthority = "";
        if (authority != null) {
          // anonymize the authority
          NodeName hostName = new NodeName(null, uri.getHost());
          anonymizedAuthority = hostName.getAnonymizedValue(statePool, conf);
        }
        ret = scheme + "://" + anonymizedAuthority + ret;
      }
    } catch (URISyntaxException use) {
      throw new RuntimeException (use);
    }
    
    return ret;
  }
  
  // Anonymize the file-path
  private static String anonymizePath(String path, WordList dState, 
                                      WordList fState) {
    StringBuilder buffer = new StringBuilder();
    StringTokenizer tokenizer = new StringTokenizer(path, Path.SEPARATOR, true);
    while (tokenizer.hasMoreTokens()) {
      String token = tokenizer.nextToken();
      if (Path.SEPARATOR.equals(token)) {
        buffer.append(token);
      } else if (Path.CUR_DIR.equals(token)) {
        buffer.append(token);
      } else if (PREV_DIR.equals(token)) {
        buffer.append(token);
      } else if (tokenizer.hasMoreTokens()){
        // this component is a directory
        buffer.append(anonymize(token, dState));
      } else {
        // this component is a file
        buffer.append(anonymize(token, fState));
      }
    }
    
    return buffer.toString();
  }
  
  //TODO There is no caching for saving memory.
  private static String anonymize(String data, WordList wordList) {
    if (data == null) {
      return null;
    }

    if (WordListAnonymizerUtility.needsAnonymization(data)) {
      String suffix = "";
      String coreData = data;
      // check and extract suffix
      if (WordListAnonymizerUtility.hasSuffix(data, KNOWN_SUFFIXES)) {
        // check if the data ends with a known suffix
        String[] split = 
          WordListAnonymizerUtility.extractSuffix(data, KNOWN_SUFFIXES);
        suffix = split[1];
        coreData = split[0];
      }

      // check if the data is known content
      //TODO [Chunking] Do this for sub-strings of data
      String anonymizedData = coreData;
      if (!WordListAnonymizerUtility.isKnownData(coreData)) {
        if (!wordList.contains(coreData)) {
          wordList.add(coreData);
        }
        anonymizedData  = wordList.getName() + wordList.indexOf(coreData);
      }

      return anonymizedData + suffix;
    } else {
      return data;
    }
  }
}