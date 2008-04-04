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

package org.apache.hadoop.ipc;

import java.io.IOException;

public class RemoteException extends IOException {
  private String className;
  
  public RemoteException(String className, String msg) {
    super(msg);
    this.className = className;
  }
  
  public String getClassName() {
    return className;
  }

  /**
   * If this remote exception wraps up one of the lookupTypes
   * then return this exception.
   * <p>
   * Unwraps any IOException that has a default constructor.
   * 
   * @param lookupTypes the desired exception class.
   * @return IOException, which is either the lookupClass exception or this.
   */
  public IOException unwrapRemoteException(Class... lookupTypes) {
    if(lookupTypes == null)
      return this;
    for(Class lookupClass : lookupTypes) {
      if(!IOException.class.isAssignableFrom(lookupClass))
        continue;
      if(lookupClass.getName().equals(getClassName())) {
        try {
          IOException ex = (IOException)lookupClass.newInstance();
          ex.initCause(this);
          return ex;
        } catch(Exception e) {
          // cannot instantiate lookupClass, just return this
          return this;
        }
      } 
    }
    return this;
  }

  /**
   * If this remote exception wraps an IOException that has a default
   * contructor then instantiate and return the original exception.
   * Otherwise return this.
   * 
   * @return IOException
   */
  public IOException unwrapRemoteException() {
    IOException ex;
    try {
      Class realClass = Class.forName(getClassName());
      if(!IOException.class.isAssignableFrom(realClass))
        return this;
      ex = (IOException)realClass.newInstance();
      ex.initCause(this);
      return ex;
    } catch(Exception e) {
      // cannot instantiate the original exception, just throw this
    }
    return this;
  }
}
