/**
 * Copyright 2008 The Apache Software Foundation
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
package org.apache.hadoop.hbase.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A facade for a {@link org.apache.hadoop.hbase.io.hfile.HFile.Reader} that serves up
 * either the top or bottom half of a HFile where 'bottom' is the first half
 * of the file containing the keys that sort lowest and 'top' is the second half
 * of the file with keys that sort greater than those of the bottom half.
 * The top includes the split files midkey, of the key that follows if it does
 * not exist in the file.
 *
 * <p>This type works in tandem with the {@link Reference} type.  This class
 * is used reading while Reference is used writing.
 *
 * <p>This file is not splitable.  Calls to {@link #midkey()} return null.
 */
public class HalfStoreFileReader extends StoreFile.Reader {
  final Log LOG = LogFactory.getLog(HalfStoreFileReader.class);
  final boolean top;
  // This is the key we split around.  Its the first possible entry on a row:
  // i.e. empty column and a timestamp of LATEST_TIMESTAMP.
  protected final byte [] splitkey;

  /**
   * @param fs
   * @param p
   * @param c
   * @param r
   * @throws IOException
   */
  public HalfStoreFileReader(final FileSystem fs, final Path p, final BlockCache c,
    final Reference r)
  throws IOException {
    super(fs, p, c, false);
    // This is not actual midkey for this half-file; its just border
    // around which we split top and bottom.  Have to look in files to find
    // actual last and first keys for bottom and top halves.  Half-files don't
    // have an actual midkey themselves. No midkey is how we indicate file is
    // not splittable.
    this.splitkey = r.getSplitKey();
    // Is it top or bottom half?
    this.top = Reference.isTopFileRegion(r.getFileRegion());
  }

  protected boolean isTop() {
    return this.top;
  }

  @Override
  public HFileScanner getScanner(final boolean cacheBlocks, final boolean pread) {
    final HFileScanner s = super.getScanner(cacheBlocks, pread);
    return new HFileScanner() {
      final HFileScanner delegate = s;
      public boolean atEnd = false;

      public ByteBuffer getKey() {
        if (atEnd) return null;
        return delegate.getKey();
      }

      public String getKeyString() {
        if (atEnd) return null;

        return delegate.getKeyString();
      }

      public ByteBuffer getValue() {
        if (atEnd) return null;

        return delegate.getValue();
      }

      public String getValueString() {
        if (atEnd) return null;

        return delegate.getValueString();
      }

      public KeyValue getKeyValue() {
        if (atEnd) return null;

        return delegate.getKeyValue();
      }

      public boolean next() throws IOException {
        if (atEnd) return false;

        boolean b = delegate.next();
        if (!b) {
          return b;
        }
        // constrain the bottom.
        if (!top) {
          ByteBuffer bb = getKey();
          if (getComparator().compare(bb.array(), bb.arrayOffset(), bb.limit(),
              splitkey, 0, splitkey.length) >= 0) {
            atEnd = true;
            return false;
          }
        }
        return true;
      }

      public boolean seekBefore(byte[] key) throws IOException {
        return seekBefore(key, 0, key.length);
      }

      public boolean seekBefore(byte [] key, int offset, int length)
      throws IOException {
        if (top) {
          if (getComparator().compare(key, offset, length, splitkey, 0,
              splitkey.length) < 0) {
            return false;
          }
        } else {
          if (getComparator().compare(key, offset, length, splitkey, 0,
              splitkey.length) >= 0) {
            return seekBefore(splitkey, 0, splitkey.length);
          }
        }
        return this.delegate.seekBefore(key, offset, length);
      }

      public boolean seekTo() throws IOException {
        if (top) {
          int r = this.delegate.seekTo(splitkey);
          if (r < 0) {
            // midkey is < first key in file
            return this.delegate.seekTo();
          }
          if (r > 0) {
            return this.delegate.next();
          }
          return true;
        }

        boolean b = delegate.seekTo();
        if (!b) {
          return b;
        }
        // Check key.
        ByteBuffer k = this.delegate.getKey();
        return this.delegate.getReader().getComparator().
          compare(k.array(), k.arrayOffset(), k.limit(),
            splitkey, 0, splitkey.length) < 0;
      }

      public int seekTo(byte[] key) throws IOException {
        return seekTo(key, 0, key.length);
      }

      public int seekTo(byte[] key, int offset, int length) throws IOException {
        if (top) {
          if (getComparator().compare(key, offset, length, splitkey, 0,
              splitkey.length) < 0) {
            return -1;
          }
        } else {
          if (getComparator().compare(key, offset, length, splitkey, 0,
              splitkey.length) >= 0) {
            // we would place the scanner in the second half.
            // it might be an error to return false here ever...
            boolean res = delegate.seekBefore(splitkey, 0, splitkey.length);
            if (!res) {
              throw new IOException("Seeking for a key in bottom of file, but key exists in top of file, failed on seekBefore(midkey)");
            }
            return 1;
          }
        }
        return delegate.seekTo(key, offset, length);
      }

      @Override
      public int reseekTo(byte[] key) throws IOException {
        return reseekTo(key, 0, key.length);
      }

      @Override
      public int reseekTo(byte[] key, int offset, int length)
      throws IOException {
        //This function is identical to the corresponding seekTo function except
        //that we call reseekTo (and not seekTo) on the delegate.
        if (top) {
          if (getComparator().compare(key, offset, length, splitkey, 0,
              splitkey.length) < 0) {
            return -1;
          }
        } else {
          if (getComparator().compare(key, offset, length, splitkey, 0,
              splitkey.length) >= 0) {
            // we would place the scanner in the second half.
            // it might be an error to return false here ever...
            boolean res = delegate.seekBefore(splitkey, 0, splitkey.length);
            if (!res) {
              throw new IOException("Seeking for a key in bottom of file, but" +
                  " key exists in top of file, failed on seekBefore(midkey)");
            }
            return 1;
          }
        }
        return delegate.reseekTo(key, offset, length);
      }

      public org.apache.hadoop.hbase.io.hfile.HFile.Reader getReader() {
        return this.delegate.getReader();
      }

      public boolean isSeeked() {
        return this.delegate.isSeeked();
      }
    };
  }

  @Override
  public byte[] getLastKey() {
    if (top) {
      return super.getLastKey();
    }
    // Get a scanner that caches the block and that uses pread.
    HFileScanner scanner = getScanner(true, true);
    try {
      if (scanner.seekBefore(this.splitkey)) {
        return Bytes.toBytes(scanner.getKey());
      }
    } catch (IOException e) {
      LOG.warn("Failed seekBefore " + Bytes.toString(this.splitkey), e);
    }
    return null;
  }

  @Override
  public byte[] midkey() throws IOException {
    // Returns null to indicate file is not splitable.
    return null;
  }
}
