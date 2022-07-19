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

package org.apache.hadoop.fs.azurebfs.security;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.util.Arrays;

public class ABFSKey implements SecretKey {
    private byte[] bytes;
    private String base64Encoding;
    private byte[] sha256Hash;
    public ABFSKey(byte[] bytes) {
        if(bytes != null) {
            this.bytes = bytes.clone();
            base64Encoding = EncodingHelper.getBase64EncodedString(this.bytes);
            sha256Hash = EncodingHelper.getSHA256Hash(this.bytes);
        }
    }

    @Override
    public String getAlgorithm() {
        return null;
    }

    @Override
    public String getFormat() {
        return null;
    }

    /**
     * This method to be called by implementations of EncryptionContextProvider interface.
     * Method returns clone of the original bytes array to prevent findbugs flags.
     * */
    @Override
    public byte[] getEncoded() {
        if(bytes == null) {
            return null;
        }
        return bytes.clone();
    }

    public String getBase64EncodedString() {
        return base64Encoding;
    }

    public byte[] getSHA256Hash() {
        if(sha256Hash == null) {
            return null;
        }
        return sha256Hash.clone();
    }

    @Override
    public void destroy() {
        Arrays.fill(bytes, (byte) 0);
    }
}
