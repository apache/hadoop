/**
 * Copyright 2005 The Apache Software Foundation
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

package org.apache.hadoop.util;

import java.util.logging.*;
import java.io.*;
import java.text.*;
import java.util.Date;

/** Prints just the date and the log message. */

public class LogFormatter extends Formatter {
  private static final String FORMAT = "yyMMdd HHmmss";
  private static final String NEWLINE = System.getProperty("line.separator");

  private final Date date = new Date();
  private final SimpleDateFormat formatter = new SimpleDateFormat(FORMAT);

  private static boolean loggedSevere= false;

  private static boolean showTime = true;
  private static boolean showThreadIDs = false;

  // install when this class is loaded
  static {
    Handler[] handlers = LogFormatter.getLogger("").getHandlers();
    for (int i = 0; i < handlers.length; i++) {
      handlers[i].setFormatter(new LogFormatter());
      handlers[i].setLevel(Level.FINEST);
    }
  }

  /** Gets a logger and, as a side effect, installs this as the default
   * formatter. */
  public static Logger getLogger(String name) {
    // just referencing this class installs it
    return Logger.getLogger(name);
  }
  
  /** When true, time is logged with each entry. */
  public static void showTime(boolean showTime) {
    LogFormatter.showTime = showTime;
  }

  /** When set true, thread IDs are logged. */
  public static void setShowThreadIDs(boolean showThreadIDs) {
    LogFormatter.showThreadIDs = showThreadIDs;
  }

  /**
   * Format the given LogRecord.
   * @param record the log record to be formatted.
   * @return a formatted log record
   */
  public synchronized String format(LogRecord record) {
    StringBuffer buffer = new StringBuffer();

    // the date
    if (showTime) {
      date.setTime(record.getMillis());
      formatter.format(date, buffer, new FieldPosition(0));
    }
    
    // the thread id
    if (showThreadIDs) {
      buffer.append(" ");
      buffer.append(record.getThreadID());
    }

    // handle SEVERE specially
    if (record.getLevel() == Level.SEVERE) {
      buffer.append(" SEVERE");                   // flag it in log
      loggedSevere= true;                         // set global flag
    }

    // the message
    buffer.append(" ");
    buffer.append(formatMessage(record));

    buffer.append(NEWLINE);

    if (record.getThrown() != null) {
      try {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        record.getThrown().printStackTrace(pw);
        pw.close();
        buffer.append(sw.toString());
      } catch (Exception ex) {
      }
    }
    return buffer.toString();
  }

  /**
   * Returns <code>true</code> if this <code>LogFormatter</code> has
   * logged something at <code>Level.SEVERE</code>
   */
  public static boolean hasLoggedSevere() {
    return loggedSevere;
  }

  /** Returns a stream that, when written to, adds log lines. */
  public static PrintStream getLogStream(final Logger logger,
                                         final Level level) {
    return new PrintStream(new ByteArrayOutputStream() {
        private int scan = 0;

        private boolean hasNewline() {
          for (; scan < count; scan++) {
            if (buf[scan] == '\n')
              return true;
          }
          return false;
        }

        public void flush() throws IOException {
          if (!hasNewline())
            return;
          logger.log(level, toString().trim());
          reset();
          scan = 0;
        }
      }, true);
  }
}
