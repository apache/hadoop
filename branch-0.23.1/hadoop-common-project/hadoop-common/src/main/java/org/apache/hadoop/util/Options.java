/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.util;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

/**
 * This class allows generic access to variable length type-safe parameter
 * lists.
 */
public class Options {

  public static abstract class StringOption {
    private final String value;
    protected StringOption(String value) {
      this.value = value;
    }
    public String getValue() {
      return value;
    }
  }

  public static abstract class ClassOption {
    private final Class<?> value;
    protected ClassOption(Class<?> value) {
      this.value = value;
    }
    public Class<?> getValue() {
      return value;
    }
  }

  public static abstract class BooleanOption {
    private final boolean value;
    protected BooleanOption(boolean value) {
      this.value = value;
    }
    public boolean getValue() {
      return value;
    }
  }

  public static abstract class IntegerOption {
    private final int value;
    protected IntegerOption(int value) {
      this.value = value;
    }
    public int getValue() {
      return value;
    }
  }

  public static abstract class LongOption {
    private final long value;
    protected LongOption(long value) {
      this.value = value;
    }
    public long getValue() {
      return value;
    }
  }

  public static abstract class PathOption {
    private final Path value;
    protected PathOption(Path value) {
      this.value = value;
    }
    public Path getValue() {
      return value;
    }
  }

  public static abstract class FSDataInputStreamOption {
    private final FSDataInputStream value;
    protected FSDataInputStreamOption(FSDataInputStream value) {
      this.value = value;
    }
    public FSDataInputStream getValue() {
      return value;
    }
  }

  public static abstract class FSDataOutputStreamOption {
    private final FSDataOutputStream value;
    protected FSDataOutputStreamOption(FSDataOutputStream value) {
      this.value = value;
    }
    public FSDataOutputStream getValue() {
      return value;
    }
  }

  public static abstract class ProgressableOption {
    private final Progressable value;
    protected ProgressableOption(Progressable value) {
      this.value = value;
    }
    public Progressable getValue() {
      return value;
    }
  }

  /**
   * Find the first option of the required class.
   * @param <T> the static class to find
   * @param <base> the parent class of the array
   * @param cls the dynamic class to find
   * @param opts the list of options to look through
   * @return the first option that matches
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static <base, T extends base> T getOption(Class<T> cls, base [] opts
                                                   ) throws IOException {
    for(base o: opts) {
      if (o.getClass() == cls) {
        return (T) o;
      }
    }
    return null;
  }

  /**
   * Prepend some new options to the old options
   * @param <T> the type of options
   * @param oldOpts the old options
   * @param newOpts the new options
   * @return a new array of options
   */
  public static <T> T[] prependOptions(T[] oldOpts, T... newOpts) {
    // copy the new options to the front of the array
    T[] result = Arrays.copyOf(newOpts, newOpts.length+oldOpts.length);
    // now copy the old options
    System.arraycopy(oldOpts, 0, result, newOpts.length, oldOpts.length);
    return result;
  }
}
