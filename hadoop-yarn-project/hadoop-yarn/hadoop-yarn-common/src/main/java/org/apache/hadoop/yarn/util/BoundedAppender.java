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

package org.apache.hadoop.yarn.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A {@link CharSequence} appender that considers its {@link #limit} as upper
 * bound.
 * <p>
 * When {@link #limit} would be reached on append, past messages will be
 * truncated from head, and a header telling the user about truncation will be
 * prepended, with ellipses in between header and messages.
 * <p>
 * Note that header and ellipses are not counted against {@link #limit}.
 * <p>
 * An example:
 *
 * <pre>
 * {@code
 *   // At the beginning it's an empty string
 *   final Appendable shortAppender = new BoundedAppender(80);
 *   // The whole message fits into limit
 *   shortAppender.append(
 *       "message1 this is a very long message but fitting into limit\n");
 *   // The first message is truncated, the second not
 *   shortAppender.append("message2 this is shorter than the previous one\n");
 *   // The first message is deleted, the second truncated, the third
 *   // preserved
 *   shortAppender.append("message3 this is even shorter message, maybe.\n");
 *   // The first two are deleted, the third one truncated, the last preserved
 *   shortAppender.append("message4 the shortest one, yet the greatest :)");
 *   // Current contents are like this:
 *   // Diagnostic messages truncated, showing last 80 chars out of 199:
 *   // ...s is even shorter message, maybe.
 *   // message4 the shortest one, yet the greatest :)
 * }
 * </pre>
 * <p>
 * Note that <tt>null</tt> values are {@link #append(CharSequence) append}ed
 * just like in {@link StringBuilder#append(CharSequence) original
 * implementation}.
 * <p>
 * Note that this class is not thread safe.
 */

@InterfaceAudience.Public
@InterfaceStability.Unstable
@VisibleForTesting
public class BoundedAppender {
  @VisibleForTesting
  public static final String TRUNCATED_MESSAGES_TEMPLATE =
      "Diagnostic messages truncated, showing last "
          + "%d chars out of %d:%n...%s";

  private final int limit;
  private final StringBuilder messages = new StringBuilder();
  private int totalCharacterCount = 0;

  public BoundedAppender(final int limit) {
    Preconditions.checkArgument(limit > 0, "limit should be positive");

    this.limit = limit;
  }

  /**
   * Append a {@link CharSequence} considering {@link #limit}, truncating
   * from the head of {@code csq} or {@link #messages} when necessary.
   *
   * @param csq the {@link CharSequence} to append
   * @return this
   */
  public BoundedAppender append(final CharSequence csq) {
    appendAndCount(csq);
    checkAndCut();

    return this;
  }

  private void appendAndCount(final CharSequence csq) {
    final int before = messages.length();
    messages.append(csq);
    final int after = messages.length();
    totalCharacterCount += after - before;
  }

  private void checkAndCut() {
    if (messages.length() > limit) {
      final int newStart = messages.length() - limit;
      messages.delete(0, newStart);
    }
  }

  /**
   * Get current length of messages considering truncates
   * without header and ellipses.
   *
   * @return current length
   */
  public int length() {
    return messages.length();
  }

  public int getLimit() {
    return limit;
  }

  /**
   * Get a string representation of the actual contents, displaying also a
   * header and ellipses when there was a truncate.
   *
   * @return String representation of the {@link #messages}
   */
  @Override
  public String toString() {
    if (messages.length() < totalCharacterCount) {
      return String.format(TRUNCATED_MESSAGES_TEMPLATE, messages.length(),
          totalCharacterCount, messages.toString());
    }

    return messages.toString();
  }
}
