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

package org.apache.hadoop.io.retry;

import java.io.IOException;

import javax.security.sasl.SaslException;

import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.AccessControlException;

/**
 * The methods of UnreliableInterface could throw exceptions in a
 * predefined way. It is currently used for testing {@link RetryPolicy}
 * and {@link FailoverProxyProvider} classes, but can be potentially used
 * to test any class's behaviour where an underlying interface or class
 * may throw exceptions.
 *
 * Some methods may be annotated with the {@link Idempotent} annotation.
 * In order to test those some methods of UnreliableInterface are annotated,
 * but they are not actually Idempotent functions.
 *
 */
public interface UnreliableInterface {
  
  public static class UnreliableException extends Exception {
    private static final long serialVersionUID = 1L;

    private String identifier;
    
    public UnreliableException() {
      // no body
    }
    
    public UnreliableException(String identifier) {
      this.identifier = identifier;
    }
    
    @Override
    public String getMessage() {
      return identifier;
    }
  }
  
  public static class FatalException extends UnreliableException {
    private static final long serialVersionUID = 1L;
    // no body
  }
  
  void alwaysSucceeds() throws UnreliableException;
  
  void alwaysFailsWithFatalException() throws FatalException;
  void alwaysFailsWithRemoteFatalException() throws RemoteException;

  void failsOnceWithIOException() throws IOException;
  void failsOnceWithRemoteException() throws RemoteException;

  void failsOnceThenSucceeds() throws UnreliableException;
  boolean failsOnceThenSucceedsWithReturnValue() throws UnreliableException;

  void failsTenTimesThenSucceeds() throws UnreliableException;

  void failsWithSASLExceptionTenTimes() throws SaslException;

  @Idempotent
  void failsWithAccessControlExceptionEightTimes()
      throws AccessControlException;

  @Idempotent
  void failsWithWrappedAccessControlException()
      throws IOException;

  public String succeedsOnceThenFailsReturningString()
      throws UnreliableException, StandbyException, IOException;
  @Idempotent
  public String succeedsOnceThenFailsReturningStringIdempotent()
      throws UnreliableException, StandbyException, IOException;
  public String succeedsTenTimesThenFailsReturningString()
      throws UnreliableException, StandbyException, IOException;
  
  @Idempotent
  public String failsIfIdentifierDoesntMatch(String identifier)
      throws UnreliableException, StandbyException, IOException;

  void nonIdempotentVoidFailsIfIdentifierDoesntMatch(String identifier)
      throws UnreliableException, StandbyException, IOException;
}
