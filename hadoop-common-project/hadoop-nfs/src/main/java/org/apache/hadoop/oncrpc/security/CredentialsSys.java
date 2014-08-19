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
package org.apache.hadoop.oncrpc.security;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.oncrpc.XDR;

/** Credential used by AUTH_SYS */
public class CredentialsSys extends Credentials {
 
  private static final String HOSTNAME;
  static {
    try {
      String s = InetAddress.getLocalHost().getHostName();
      HOSTNAME = s;
      if(LOG.isDebugEnabled()) {
        LOG.debug("HOSTNAME = " + HOSTNAME);
      }
    } catch (UnknownHostException e) {
      LOG.error("Error setting HOSTNAME", e);
      throw new RuntimeException(e);
    }
  }
  
  protected int mUID, mGID;
  protected int[] mAuxGIDs;
  protected String mHostName;
  protected int mStamp;

  public CredentialsSys() {
    super(AuthFlavor.AUTH_SYS);
    this.mCredentialsLength = 0;
    this.mHostName = HOSTNAME;
  }
  
  public int getGID() {
    return mGID;
  }

  public int getUID() {
    return mUID;
  }

  public int[] getAuxGIDs() {
    return mAuxGIDs;
  }

  public void setGID(int gid) {
    this.mGID = gid;
  }

  public void setUID(int uid) {
    this.mUID = uid;
  }

  public void setStamp(int stamp) {
    this.mStamp = stamp;
  }

  @Override
  public void read(XDR xdr) {
    mCredentialsLength = xdr.readInt();

    mStamp = xdr.readInt();
    mHostName = xdr.readString();
    mUID = xdr.readInt();
    mGID = xdr.readInt();

    int length = xdr.readInt();
    mAuxGIDs = new int[length];
    for (int i = 0; i < length; i++) {
      mAuxGIDs[i] = xdr.readInt();
    }
  }

  @Override
  public void write(XDR xdr) {
    // mStamp + mHostName.length + mHostName + mUID + mGID + mAuxGIDs.count
    mCredentialsLength = 20 + mHostName.getBytes().length;
    // mAuxGIDs
    if (mAuxGIDs != null && mAuxGIDs.length > 0) {
      mCredentialsLength += mAuxGIDs.length * 4;
    }
    xdr.writeInt(mCredentialsLength);
    
    xdr.writeInt(mStamp);
    xdr.writeString(mHostName);
    xdr.writeInt(mUID);
    xdr.writeInt(mGID);
    
    if((mAuxGIDs == null) || (mAuxGIDs.length == 0)) {
      xdr.writeInt(0);
    } else {
      xdr.writeInt(mAuxGIDs.length);
      for (int i = 0; i < mAuxGIDs.length; i++) {
        xdr.writeInt(mAuxGIDs[i]);
      }
    }
  }

}
