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
 * For the usage and purpose of this class see {@link UnreliableInterface}
 * which this class implements.
 *
 * @see UnreliableInterface
 */
class UnreliableImplementation implements UnreliableInterface {

  private int failsOnceInvocationCount,
    failsOnceWithValueInvocationCount,
    failsOnceIOExceptionInvocationCount,
    failsOnceRemoteExceptionInvocationCount,
    failsTenTimesInvocationCount,
    failsWithSASLExceptionTenTimesInvocationCount,
    failsWithAccessControlExceptionInvocationCount,
    succeedsOnceThenFailsCount,
    succeedsOnceThenFailsIdempotentCount,
    succeedsTenTimesThenFailsCount;
  
  private String identifier;
  private TypeOfExceptionToFailWith exceptionToFailWith;
  
  public enum TypeOfExceptionToFailWith {
    UNRELIABLE_EXCEPTION,
    STANDBY_EXCEPTION,
    IO_EXCEPTION,
    REMOTE_EXCEPTION
  }
  
  public UnreliableImplementation() {
    this(null);
  }
  
  public UnreliableImplementation(String identifier) {
    this(identifier, TypeOfExceptionToFailWith.UNRELIABLE_EXCEPTION);
  }
  
  public void setIdentifier(String identifier) {
    this.identifier = identifier;
  }
  
  public UnreliableImplementation(String identifier,
      TypeOfExceptionToFailWith exceptionToFailWith) {
    this.identifier = identifier;
    this.exceptionToFailWith = exceptionToFailWith;
  }
  
  @Override
  public void alwaysSucceeds() {
    // do nothing
  }
  
  @Override
  public void alwaysFailsWithFatalException() throws FatalException {
    throw new FatalException();
  }
  
  @Override
  public void alwaysFailsWithRemoteFatalException() throws RemoteException {
    throw new RemoteException(FatalException.class.getName(), "Oops");
  }

  @Override
  public void failsOnceThenSucceeds() throws UnreliableException {
    if (failsOnceInvocationCount++ == 0) {
      throw new UnreliableException();
    }
  }

  @Override
  public boolean failsOnceThenSucceedsWithReturnValue() throws UnreliableException {
    if (failsOnceWithValueInvocationCount++ == 0) {
      throw new UnreliableException();
    }
    return true;
  }

  @Override
  public void failsOnceWithIOException() throws IOException {
    if (failsOnceIOExceptionInvocationCount++ == 0) {
      throw new IOException("test exception for failsOnceWithIOException");
    }
  }

  @Override
  public void failsOnceWithRemoteException() throws RemoteException {
    if (failsOnceRemoteExceptionInvocationCount++ == 0) {
      throw new RemoteException(IOException.class.getName(),
          "test exception for failsOnceWithRemoteException");
    }
  }

  @Override
  public void failsTenTimesThenSucceeds() throws UnreliableException {
    if (failsTenTimesInvocationCount++ < 10) {
      throw new UnreliableException();
    }
  }

  @Override
  public void failsWithSASLExceptionTenTimes() throws SaslException {
    if (failsWithSASLExceptionTenTimesInvocationCount ++ < 10) {
      throw new SaslException();
    }
  }

  @Override
  public void failsWithAccessControlExceptionEightTimes()
      throws AccessControlException {
    if (failsWithAccessControlExceptionInvocationCount++ < 8) {
      throw new AccessControlException();
    }
  }

  public void failsWithWrappedAccessControlException()
      throws IOException {
    AccessControlException ace = new AccessControlException();
    IOException ioe = new IOException(ace);
    throw new IOException(ioe);
  }

  @Override
  public String succeedsOnceThenFailsReturningString()
      throws UnreliableException, IOException, StandbyException {
    if (succeedsOnceThenFailsCount++ < 1) {
      return identifier;
    } else {
      throwAppropriateException(exceptionToFailWith, identifier);
      return null;
    }
  }

  @Override
  public String succeedsTenTimesThenFailsReturningString()
      throws UnreliableException, IOException, StandbyException {
    if (succeedsTenTimesThenFailsCount++ < 10) {
      return identifier;
    } else {
      throwAppropriateException(exceptionToFailWith, identifier);
      return null;
    }
  }

  @Override
  public String succeedsOnceThenFailsReturningStringIdempotent()
      throws UnreliableException, StandbyException, IOException {
    if (succeedsOnceThenFailsIdempotentCount++ < 1) {
      return identifier;
    } else {
      throwAppropriateException(exceptionToFailWith, identifier);
      return null;
    }
  }

  @Override
  public String failsIfIdentifierDoesntMatch(String identifier)
      throws UnreliableException, StandbyException, IOException {
    if (this.identifier.equals(identifier)) {
      return identifier;
    } else {
      String message = "expected '" + this.identifier + "' but received '" +
          identifier + "'";
      throwAppropriateException(exceptionToFailWith, message);
      return null;
    }
  }
  
  @Override
  public void nonIdempotentVoidFailsIfIdentifierDoesntMatch(String identifier)
      throws UnreliableException, StandbyException, IOException {
    if (this.identifier.equals(identifier)) {
      return;
    } else {
      String message = "expected '" + this.identifier + "' but received '" +
          identifier + "'";
      throwAppropriateException(exceptionToFailWith, message);
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[" + identifier + "]";
  }

  private static void throwAppropriateException(TypeOfExceptionToFailWith eType,
      String message) throws UnreliableException, StandbyException, IOException {
    switch (eType) {
    case STANDBY_EXCEPTION:
      throw new StandbyException(message);
    case UNRELIABLE_EXCEPTION:
      throw new UnreliableException(message);
    case IO_EXCEPTION:
      throw new IOException(message);
    case REMOTE_EXCEPTION:
      throw new RemoteException(IOException.class.getName(), message);
    default:
      throw new RuntimeException(message);
    }
  }
}
