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

package org.apache.hadoop.yarn.exceptions;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;

public abstract class YarnRemoteException extends IOException {
  private static final long serialVersionUID = 1L;
  
  public YarnRemoteException() {
    super();
  }
  
  public YarnRemoteException(String message, Throwable cause) {
    super(message, cause);
  }
  
  public YarnRemoteException(Throwable cause) {
    super(cause);
  }
  
  public YarnRemoteException(String message) {
    super(message);
  }
  
  @Override
  public void printStackTrace(PrintWriter pw) {
    pw.append("RemoteTrace: \n").append(getRemoteTrace())
      .append(" at LocalTrace: \n\t");
    super.printStackTrace(pw);
  }

  @Override
  public void printStackTrace(PrintStream ps) {
    ps.append("RemoteTrace: \n").append(getRemoteTrace())
      .append(" at Local Trace: \n\t");
    super.printStackTrace(ps);
  }
  
  public abstract String getRemoteTrace();
  
  public abstract YarnRemoteException getCause();
}