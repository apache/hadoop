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
import org.apache.hadoop.tools.rumen.ParsedHost;
import org.apache.hadoop.tools.rumen.anonymization.WordList;
import org.apache.hadoop.tools.rumen.state.State;
import org.apache.hadoop.tools.rumen.state.StatePool;
import org.codehaus.jackson.annotate.JsonIgnore;

/**
 * Represents the cluster host.
 */
public class NodeName implements AnonymizableDataType<String> {
  private String hostName;
  private String rackName;
  private String nodeName;
  private String anonymizedNodeName;
  
  public static final NodeName ROOT = new NodeName("<root>");
  
  /**
   * A composite state for node-name.
   */
  public static class NodeNameState implements State {
    private WordList rackNameState = new WordList("rack");
    private WordList hostNameState =  new WordList("host");
    
    @Override
    @JsonIgnore
    public boolean isUpdated() {
      return rackNameState.isUpdated() || hostNameState.isUpdated();
    }
    
    public WordList getRackNameState() {
      return rackNameState;
    }
    
    public WordList getHostNameState() {
      return hostNameState;
    }
    
    public void setRackNameState(WordList state) {
      this.rackNameState = state;
    }
    
    public void setHostNameState(WordList state) {
      this.hostNameState = state;
    }
    
    @Override
    public String getName() {
      return "node";
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
  
  public NodeName(String nodeName) {
    this.nodeName = nodeName;
    ParsedHost pHost = ParsedHost.parse(nodeName);
    if (pHost == null) {
      this.rackName = null;
      this.hostName = nodeName;
    } else {
      //TODO check for null and improve .. possibly call NodeName(r,h)
      this.rackName = pHost.getRackName();
      this.hostName = pHost.getNodeName();
    }
  }
  
  public NodeName(String rName, String hName) {
    rName = (rName == null || rName.length() == 0) ? null : rName;
    hName = (hName == null || hName.length() == 0) ? null : hName;
    if (hName == null) {
      nodeName = rName;
      rackName = rName;
    } else if (rName == null) {
      nodeName = hName;
      ParsedHost pHost = ParsedHost.parse(nodeName);
      if (pHost == null) {
        this.rackName = null;
        this.hostName = hName;
      } else {
        this.rackName = pHost.getRackName();
        this.hostName = pHost.getNodeName();
      }
    } else {
      rackName = rName;
      this.hostName = hName;
      this.nodeName = "/" + rName + "/" + hName;
    }
  }
  
  public String getHostName() {
    return hostName;
  }
  
  public String getRackName() {
    return rackName;
  }
  
  @Override
  public String getValue() {
    return nodeName;
  }
  
  @Override
  public String getAnonymizedValue(StatePool statePool, Configuration conf) {
    if (this.getValue().equals(ROOT.getValue())) {
      return getValue();
    }
    if (anonymizedNodeName == null) {
      anonymize(statePool);
    }
    return anonymizedNodeName;
  }
  
  private void anonymize(StatePool pool) {
    StringBuffer buf = new StringBuffer();
    NodeNameState state = (NodeNameState) pool.getState(getClass());
    if (state == null) {
      state = new NodeNameState();
      pool.addState(getClass(), state);
    }
    
    if (rackName != null && hostName != null) {
      buf.append('/');
      buf.append(anonymize(rackName, state.getRackNameState()));
      buf.append('/');
      buf.append(anonymize(hostName, state.getHostNameState()));
    } else {
      if (state.getRackNameState().contains(nodeName) || rackName != null) {
        buf.append(anonymize(nodeName, state.getRackNameState()));
      } else {
        buf.append(anonymize(nodeName, state.getHostNameState()));
      }
    }
    
    anonymizedNodeName = buf.toString();
  }
  
  //TODO There is no caching for saving memory.
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