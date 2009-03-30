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
package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** This writable component builds from a throwable */

public final class ThrowableWritable implements Writable {

  /** throwable classname */
  private String classname;

  /** throwable message */

  private String message;
  /** cause: may be null */
  private ThrowableWritable cause;

  /**
   * Stack trace as string; will be null when an empty element is created,
   * otherwise it will be an array of length zero.
   */
  private String[] stack;


  /**
   * Empty constructor.
   * Only use this when you are planning to deserialize data, as the object is
   * otherwise incomplete.
   */
  public ThrowableWritable() {
  }


  /**
   * Construct a lightweight throwable writeable with no stack trace; and the
   * message passed in
   *
   * @param message message to use
   */
  public ThrowableWritable(String message) {
    this.message = message;
    stack = new String[0];
    classname = "";
  }

  /**
   * recursively construct from a throwable chain.
   *
   * @param thrown The throwable chain to build this writeable from.
   */
  public ThrowableWritable(Throwable thrown) {
    classname = thrown.getClass().getName();
    message = thrown.getMessage();

    StackTraceElement[] st = thrown.getStackTrace();
    if (st != null) {
      int sl = st.length;
      stack = new String[sl];
      for (int i = 0; i < sl; i++) {
        stack[i] = st[i].toString();
      }
    } else {
      stack = new String[0];
    }
    Throwable rootCause = thrown.getCause();
    if (rootCause != null && rootCause != thrown) {
      cause = new ThrowableWritable(rootCause);
    }
  }

  /**
   * Copy constructor.
   *
   * @param that the original instance to copy
   */
  public ThrowableWritable(ThrowableWritable that) {
    classname = that.classname;
    message = that.message;
    //copy stack trace
    if (that.stack == null) {
      stack = new String[0];
    } else {
      int l = that.stack.length;
      stack = new String[l];
      System.arraycopy(that.stack, 0, stack, 0, l);
    }
    //copy any nested cause
    if (that.cause != null) {
      cause = new ThrowableWritable(that.cause);
    }
  }


  /**
   * Get the classname of the underlying throwable
   *
   * @return the classname of the original throwable
   */
  public String getClassname() {
    return classname;
  }

  /**
   * Get the text string this instance was constructed with
   *
   * @return the message of the underlying throwable
   */
  public String getMessage() {
    return message;
  }

  /**
   * Get any nested cause of the exception
   *
   * @return any nested cause as another ThrowableWritable -or null
   */
  public ThrowableWritable getCause() {
    return cause;
  }

  /**
   * Get the stack trace of the original throwable. It may be of size 0.
   *
   * @return the stack trace converted to strings
   */
  public String[] getStack() {
    return stack;
  }


  /**
   * determine (recursively) the depth of this Throwable chain
   *
   * @return a number equal to or greater than 1
   */
  public int getDepth() {
    return 1 + (cause == null ? 0 : cause.getDepth());
  }

  /**
   * {@inheritDoc}
   *
   * @param out <code>DataOutput</code> to serialize this object into.
   *
   * @throws IOException IO trouble
   */
  public void write(DataOutput out) throws IOException {
    out.writeUTF(classname);
    out.writeUTF(message);
    if (stack != null) {
      out.writeInt(stack.length);
      for (String call : stack) {
        out.writeUTF(call);
      }
    } else {
      out.writeInt(0);
    }
    //look for a cause
    boolean hasCause = cause != null;
    out.writeBoolean(hasCause);
    if (hasCause) {
      //recursively write it
      cause.write(out);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @param in <code>DataInput</code> to deseriablize this object from.
   *
   * @throws IOException IO trouble
   */
  public void readFields(DataInput in) throws IOException {
    classname = in.readUTF();
    message = in.readUTF();
    int stackLength = in.readInt();
    if (stack == null || stack.length != stackLength) {
      //create a new stack array
      stack = new String[stackLength];
    }
    //read in the stack
    for (int i = 0; i < stackLength; i++) {
      stack[i] = in.readUTF();
    }
    //look for any nested cause
    boolean hasCause = in.readBoolean();
    if (hasCause) {
      if (cause == null) {
        cause = new ThrowableWritable();
      }
      cause.readFields(in);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @throws CloneNotSupportedException this should not happen
   */
  @SuppressWarnings({"CloneDoesntCallSuperClone"})
  @Override
  protected Object clone() throws CloneNotSupportedException {
    return new ThrowableWritable(this);
  }

  /**
   * {@inheritDoc}
   *
   * The classname and message are used for equality
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ThrowableWritable that = (ThrowableWritable) o;

    if (classname != null ? !classname.equals(that.classname) : that.classname != null)
      return false;
    return !(message != null ? !message.equals(that.message) : that.message != null);

  }

  /**
   * {@inheritDoc}
   *
   * The classname and message are used in the hash
   */
  @Override
  public int hashCode() {
    int result = classname != null ? classname.hashCode() : 0;
    result = 31 * result + (message != null ? message.hashCode() : 0);
    return result;
  }

  /**
   * Return the classname and message in the format classname: message The
   * output is designed to resemble that of {@link Throwable#toString()} if the
   * message and classname are both set. If only the message is set, only that
   * is printed.
   *
   * @return a string representation of the object.
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    if (classname != null) {
      builder.append(classname);
      if (!classname.isEmpty()) {
        builder.append(": ");
      }
    }
    if (message != null) {
      builder.append(message);
    }
    return builder.toString();
  }
}
