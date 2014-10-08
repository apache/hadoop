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

package org.apache.hadoop.fs.azure;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * A simple memory key-value store to help mock the Windows Azure Storage
 * implementation for unit testing.
 */
public class InMemoryBlockBlobStore {
  private final HashMap<String, Entry> blobs = new HashMap<String, Entry>();
  private HashMap<String, String> containerMetadata;

  public synchronized Iterable<String> getKeys() {
    return new ArrayList<String>(blobs.keySet());
  }

  public static class ListBlobEntry {
    private final String key;
    private final HashMap<String, String> metadata;
    private final int contentLength;
    private final boolean isPageBlob;

    
    ListBlobEntry(String key, HashMap<String, String> metadata,
        int contentLength, boolean isPageBlob) {
      this.key = key;
      this.metadata = metadata;
      this.contentLength = contentLength;
      this.isPageBlob = isPageBlob;
    }

    public String getKey() {
      return key;
    }

    public HashMap<String, String> getMetadata() {
      return metadata;
    }

    public int getContentLength() {
      return contentLength;
    }

    public boolean isPageBlob() {
      return isPageBlob;
    }
  }

  /**
   * List all the blobs whose key starts with the given prefix.
   * 
   * @param prefix
   *          The prefix to check.
   * @param includeMetadata
   *          If set, the metadata in the returned listing will be populated;
   *          otherwise it'll be null.
   * @return The listing.
   */
  public synchronized Iterable<ListBlobEntry> listBlobs(String prefix,
      boolean includeMetadata) {
    ArrayList<ListBlobEntry> list = new ArrayList<ListBlobEntry>();
    for (Map.Entry<String, Entry> entry : blobs.entrySet()) {
      if (entry.getKey().startsWith(prefix)) {
        list.add(new ListBlobEntry(
            entry.getKey(),
            includeMetadata ?
                new HashMap<String, String>(entry.getValue().metadata) :
                  null,
            entry.getValue().content.length,
            entry.getValue().isPageBlob));
      }
    }
    return list;
  }

  public synchronized byte[] getContent(String key) {
    return blobs.get(key).content;
  }

  @SuppressWarnings("unchecked")
  public synchronized void setContent(String key, byte[] value,
      HashMap<String, String> metadata, boolean isPageBlob,
      long length) {
    blobs.put(key, new Entry(value, (HashMap<String, String>)metadata.clone(),
        isPageBlob, length));
  }

  @SuppressWarnings("unchecked")
  public synchronized void setMetadata(String key,
      HashMap<String, String> metadata) {
    blobs.get(key).metadata = (HashMap<String, String>) metadata.clone();
  }

  public OutputStream uploadBlockBlob(final String key,
      final HashMap<String, String> metadata) {
    setContent(key, new byte[0], metadata, false, 0);
    return new ByteArrayOutputStream() {
      @Override
      public void flush()
          throws IOException {
        super.flush();
        byte[] tempBytes = toByteArray();
        setContent(key, tempBytes, metadata, false, tempBytes.length);
      }
      @Override
      public void close()
          throws IOException {
        super.close();
        byte[] tempBytes = toByteArray();
        setContent(key, tempBytes, metadata, false, tempBytes.length);
      }
    };
  }

  public OutputStream uploadPageBlob(final String key,
      final HashMap<String, String> metadata,
      final long length) {
    setContent(key, new byte[0], metadata, true, length);
    return new ByteArrayOutputStream() {
      @Override
      public void flush()
          throws IOException {
        super.flush();
        setContent(key, toByteArray(), metadata, true, length);
      }
    };
  }

  public synchronized void copy(String sourceKey, String destKey) {
    blobs.put(destKey, blobs.get(sourceKey));
  }

  public synchronized void delete(String key) {
    blobs.remove(key);
  }

  public synchronized boolean exists(String key) {
    return blobs.containsKey(key);
  }

  @SuppressWarnings("unchecked")
  public synchronized HashMap<String, String> getMetadata(String key) {
    return (HashMap<String, String>) blobs.get(key).metadata.clone();
  }

  public synchronized HashMap<String, String> getContainerMetadata() {
    return containerMetadata;
  }

  public synchronized void setContainerMetadata(HashMap<String, String> metadata) {
    containerMetadata = metadata;
  }

  private static class Entry {
    private byte[] content;
    private HashMap<String, String> metadata;
    private boolean isPageBlob;
    @SuppressWarnings("unused") // TODO: use it
    private long length;

    public Entry(byte[] content, HashMap<String, String> metadata,
        boolean isPageBlob, long length) {
      this.content = content;
      this.metadata = metadata;
      this.isPageBlob = isPageBlob;
      this.length = length;
    }
  }
}
