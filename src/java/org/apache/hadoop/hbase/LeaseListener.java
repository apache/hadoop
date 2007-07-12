/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase;


/**
 * LeaseListener is an interface meant to be implemented by users of the Leases 
 * class.
 *
 * It receives events from the Leases class about the status of its accompanying
 * lease.  Users of the Leases class can use a LeaseListener subclass to, for 
 * example, clean up resources after a lease has expired.
 */
public interface LeaseListener {
  /** When a lease expires, this method is called. */
  public void leaseExpired();
}
