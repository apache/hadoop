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

package org.apache.hadoop.sqoop.lib;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.Text;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses a record containing one or more fields. Fields are separated
 * by some FIELD_DELIMITER character, e.g. a comma or a ^A character.
 * Records are terminated by a RECORD_DELIMITER character, e.g., a newline.
 *
 * Fields may be (optionally or mandatorily) enclosed by a quoting char
 * e.g., '\"'
 *
 * Fields may contain escaped characters. An escape character may be, e.g.,
 * the '\\' character. Any character following an escape character
 * is treated literally. e.g., '\n' is recorded as an 'n' character, not a
 * newline.
 *
 * Unexpected results may occur if the enclosing character escapes itself.
 * e.g., this cannot parse SQL SELECT statements where the single character
 * ['] escapes to [''].
 *
 * This class is not synchronized. Multiple threads must use separate
 * instances of RecordParser.
 *
 * The fields parsed by RecordParser are backed by an internal buffer
 * which is cleared when the next call to parseRecord() is made. If
 * the buffer is required to be preserved, you must copy it yourself.
 */
public final class RecordParser {

  public static final Log LOG = LogFactory.getLog(RecordParser.class.getName());

  private enum ParseState {
    FIELD_START,
    ENCLOSED_FIELD,
    UNENCLOSED_FIELD,
    ENCLOSED_ESCAPE,
    ENCLOSED_EXPECT_DELIMITER,
    UNENCLOSED_ESCAPE
  }

  public static class ParseError extends Exception {
    public ParseError() {
      super("ParseError");
    }

    public ParseError(final String msg) {
      super(msg);
    }

    public ParseError(final String msg, final Throwable cause) {
      super(msg, cause);
    }

    public ParseError(final Throwable cause) {
      super(cause);
    }
  }

  private char fieldDelim;
  private char recordDelim;
  private char enclosingChar;
  private char escapeChar;
  private boolean enclosingRequired;
  private ArrayList<String> outputs;

  public RecordParser(final char field, final char record, final char enclose,
      final char escape, final boolean mustEnclose) {
    this.fieldDelim = field;
    this.recordDelim = record;
    this.enclosingChar = enclose;
    this.escapeChar = escape;
    this.enclosingRequired = mustEnclose;

    this.outputs = new ArrayList<String>();
  }

  /**
   * Return a list of strings representing the fields of the input line.
   * This list is backed by an internal buffer which is cleared by the
   * next call to parseRecord().
   */
  public List<String> parseRecord(CharSequence input) throws ParseError {
    if (null == input) {
      throw new ParseError("null input string");
    }

    return parseRecord(CharBuffer.wrap(input));
  }

  /**
   * Return a list of strings representing the fields of the input line.
   * This list is backed by an internal buffer which is cleared by the
   * next call to parseRecord().
   */
  public List<String> parseRecord(Text input) throws ParseError { 
    if (null == input) { 
      throw new ParseError("null input string");
    }

    // TODO(aaron): The parser should be able to handle UTF-8 strings
    // as well, to avoid this transcode operation.
    return parseRecord(input.toString());
  }

  /**
   * Return a list of strings representing the fields of the input line.
   * This list is backed by an internal buffer which is cleared by the
   * next call to parseRecord().
   */
  public List<String> parseRecord(byte [] input) throws ParseError {
    if (null == input) {
      throw new ParseError("null input string");
    }

    return parseRecord(ByteBuffer.wrap(input).asCharBuffer());
  }

  /**
   * Return a list of strings representing the fields of the input line.
   * This list is backed by an internal buffer which is cleared by the
   * next call to parseRecord().
   */
  public List<String> parseRecord(char [] input) throws ParseError {
    if (null == input) {
      throw new ParseError("null input string");
    }

    return parseRecord(CharBuffer.wrap(input));
  }

  public List<String> parseRecord(ByteBuffer input) throws ParseError {
    if (null == input) {
      throw new ParseError("null input string");
    }

    return parseRecord(input.asCharBuffer());
  }

  /**
   * Return a list of strings representing the fields of the input line.
   * This list is backed by an internal buffer which is cleared by the
   * next call to parseRecord().
   */
  public List<String> parseRecord(CharBuffer input) throws ParseError {
    if (null == input) {
      throw new ParseError("null input string");
    }

    /*
      This method implements the following state machine to perform
      parsing.

      Note that there are no restrictions on whether particular characters
      (e.g., field-sep, record-sep, etc) are distinct or the same. The
      state transitions are processed in the order seen in this comment.

      Starting state is FIELD_START
        encloser -> ENCLOSED_FIELD
        escape char -> UNENCLOSED_ESCAPE
        field delim -> FIELD_START (for a new field)
        record delim -> stops processing
        all other letters get added to current field, -> UNENCLOSED FIELD

      ENCLOSED_FIELD state:
        escape char goes to ENCLOSED_ESCAPE
        encloser goes to ENCLOSED_EXPECT_DELIMITER
        field sep or record sep gets added to the current string
        normal letters get added to the current string

      ENCLOSED_ESCAPE state:
        any character seen here is added literally, back to ENCLOSED_FIELD

      ENCLOSED_EXPECT_DELIMITER state:
        field sep goes to FIELD_START
        record sep halts processing.
        all other characters are errors.

      UNENCLOSED_FIELD state:
        ESCAPE char goes to UNENCLOSED_ESCAPE
        FIELD_SEP char goes to FIELD_START
        RECORD_SEP char halts processing
        normal chars or the enclosing char get added to the current string

      UNENCLOSED_ESCAPE:
        add charater literal to current string, return to UNENCLOSED_FIELD
    */

    char curChar = '\000';
    ParseState state = ParseState.FIELD_START;
    int len = input.length();
    StringBuilder sb = null;

    outputs.clear();

    for (int pos = 0; pos < len; pos++) {
      curChar = input.get();
      switch (state) {
      case FIELD_START:
        // ready to start processing a new field.
        if (null != sb) {
          // We finished processing a previous field. Add to the list.
          outputs.add(sb.toString());
        }

        sb = new StringBuilder();
        if (this.enclosingChar == curChar) {
          // got an opening encloser.
          state = ParseState.ENCLOSED_FIELD;
        } else if (this.escapeChar == curChar) {
          state = ParseState.UNENCLOSED_ESCAPE;
        } else if (this.fieldDelim == curChar) {
          // we have a zero-length field. This is a no-op.
        } else if (this.recordDelim == curChar) {
          // we have a zero-length field, that ends processing.
          pos = len;
        } else {
          // current char is part of the field.
          state = ParseState.UNENCLOSED_FIELD;
          sb.append(curChar);

          if (this.enclosingRequired) {
            throw new ParseError("Opening field-encloser expected at position " + pos);
          }
        }

        break;

      case ENCLOSED_FIELD:
        if (this.escapeChar == curChar) {
          // the next character is escaped. Treat it literally.
          state = ParseState.ENCLOSED_ESCAPE;
        } else if (this.enclosingChar == curChar) {
          // we're at the end of the enclosing field. Expect an EOF or EOR char.
          state = ParseState.ENCLOSED_EXPECT_DELIMITER;
        } else {
          // this is a regular char, or an EOF / EOR inside an encloser. Add to
          // the current field string, and remain in this state.
          sb.append(curChar);
        }

        break;

      case UNENCLOSED_FIELD:
        if (this.escapeChar == curChar) {
          // the next character is escaped. Treat it literally.
          state = ParseState.UNENCLOSED_ESCAPE;
        } else if (this.fieldDelim == curChar) {
          // we're at the end of this field; may be the start of another one.
          state = ParseState.FIELD_START;
        } else if (this.recordDelim == curChar) {
          pos = len; // terminate processing immediately.
        } else {
          // this is a regular char. Add to the current field string,
          // and remain in this state.
          sb.append(curChar);
        }

        break;
        
      case ENCLOSED_ESCAPE:
        // Treat this character literally, whatever it is, and return to enclosed
        // field processing.
        sb.append(curChar);
        state = ParseState.ENCLOSED_FIELD;
        break;

      case ENCLOSED_EXPECT_DELIMITER:
        // We were in an enclosed field, but got the final encloser. Now we expect
        // either an end-of-field or an end-of-record.
        if (this.fieldDelim == curChar) {
          // end of one field is the beginning of the next.
          state = ParseState.FIELD_START;
        } else if (this.recordDelim == curChar) {
          // stop processing.
          pos = len;
        } else {
          // Don't know what to do with this character.
          throw new ParseError("Expected delimiter at position " + pos);
        }

        break;

      case UNENCLOSED_ESCAPE:
        // Treat this character literally, whatever it is, and return to non-enclosed
        // field processing.
        sb.append(curChar);
        state = ParseState.UNENCLOSED_FIELD;
        break;
      }
    }

    if (state == ParseState.FIELD_START && curChar == this.fieldDelim) {
      // we hit an EOF/EOR as the last legal character and we need to mark
      // that string as recorded. This if block is outside the for-loop since
      // we don't have a physical 'epsilon' token in our string.
      if (null != sb) {
        outputs.add(sb.toString());
        sb = new StringBuilder();
      }
    }

    if (null != sb) {
      // There was a field that terminated by running out of chars or an EOR
      // character. Add to the list.
      outputs.add(sb.toString());
    }

    return outputs;
  }


  public boolean isEnclosingRequired() { 
    return enclosingRequired;
  }

  @Override
  public String toString() {
    return "RecordParser[" + fieldDelim + ',' + recordDelim + ',' + enclosingChar + ','
        + escapeChar + ',' + enclosingRequired + "]";
  }

  @Override
  public int hashCode() {
    return this.toString().hashCode();
  }
}
