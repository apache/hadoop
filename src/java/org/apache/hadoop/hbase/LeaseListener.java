/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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


/*******************************************************************************
 * LeaseListener is a small class meant to be overridden by users of the Leases 
 * class.
 *
 * It receives events from the Leases class about the status of its accompanying
 * lease.  Users of the Leases class can use a LeaseListener subclass to, for 
 * example, clean up resources after a lease has expired.
 ******************************************************************************/
public abstract class LeaseListener {
  public LeaseListener() {
  }

  public void leaseRenewed() {
  }

  /** When the user cancels a lease, this method is called. */
  public void leaseCancelled() {
  }

  /** When a lease expires, this method is called. */
  public void leaseExpired() {
  }
}
