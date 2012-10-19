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

package org.apache.hadoop.yarn.factories.impl.pb;

import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.exceptions.impl.pb.YarnRemoteExceptionPBImpl;
import org.apache.hadoop.yarn.factories.YarnRemoteExceptionFactory;

public class YarnRemoteExceptionFactoryPBImpl implements
    YarnRemoteExceptionFactory {

  private static final YarnRemoteExceptionFactory self = new YarnRemoteExceptionFactoryPBImpl();

  private YarnRemoteExceptionFactoryPBImpl() {
  }

  public static YarnRemoteExceptionFactory get() {
    return self;
  }

  @Override
  public YarnRemoteException createYarnRemoteException(String message) {
    return new YarnRemoteExceptionPBImpl(message);
  }

  @Override
  public YarnRemoteException createYarnRemoteException(String message,
      Throwable t) {
    return new YarnRemoteExceptionPBImpl(message, t);
  }

  @Override
  public YarnRemoteException createYarnRemoteException(Throwable t) {
    return new YarnRemoteExceptionPBImpl(t);
  }
}
