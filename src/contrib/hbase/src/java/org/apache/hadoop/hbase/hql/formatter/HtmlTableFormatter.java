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
package org.apache.hadoop.hbase.hql.formatter;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;

import org.apache.hadoop.hbase.hql.TableFormatter;
import org.znerd.xmlenc.LineBreak;
import org.znerd.xmlenc.XMLOutputter;
import org.znerd.xmlenc.XMLEncoder;
import org.znerd.xmlenc.InvalidXMLException;

/**
 * Formatter that outputs data inside an HTML table. If only a single cell
 * result, then no formatting is done.  Presumption is that client manages
 * serial access outputting tables.  Does not close passed {@link Writer}.
 * Since hbase columns have no typing, the formatter presumes a type of
 * UTF-8 String.  If cells contain images, etc., this formatter will mangle
 * their display.
 * <p>TODO: Uses xmlenc. Hopefully it flushes every so often (Claims its a 
 * stream-based outputter).  Verify.
 */
public class HtmlTableFormatter implements TableFormatter {
  private final XMLOutputter outputter;
  private boolean noFormatting = false;
  private final Writer out;
  
  // Uninstantiable
  @SuppressWarnings("unused")
  private HtmlTableFormatter() {
    this(null);
  }

  /*
   * An encoder that replaces illegal XML characters with the '@' sign.
   */
  private static class HbaseXMLEncoder extends XMLEncoder {
    @SuppressWarnings("deprecation")
    public HbaseXMLEncoder()
    throws IllegalArgumentException, UnsupportedEncodingException {
      super("UTF-8");
    }
    
    @Override
    public void text(Writer w, char c, boolean escape)
    throws InvalidXMLException, IOException {
      super.text(w, legalize(c), escape);
    }
    
    @Override
    public void text(Writer w, char[] cs, int start, int length, boolean b)
        throws NullPointerException, IndexOutOfBoundsException,
        InvalidXMLException, IOException {
      for (int i = start; i < start + length; i++) {
        cs[i] = legalize(cs[i]);
      }
      super.text(w, cs, start, length, b);
    }
    
    /**
     * If character is in range A, C, or E, then replace with '@'
     * <pre>
     * A   0-8     Control characters   -- Not allowed in XML 1.0 --
     * B   9-10    Normal characters    Never needed
     * C   11-12   Control characters   -- Not allowed in XML 1.0 --
     * D   13      Normal character     Never needed
     * E   14-31   Control characters   -- Not allowed in XML 1.0 --
     * </pre>
     * @param c Character to look at.
     * @return
     */
    private char legalize(final char c) {
      return (c <= 8 || c == 11 || c == 12 || (c >= 14 && c <= 31))? '@': c;
    }
  }
  
  public HtmlTableFormatter(final Writer o) {
    this.out = o;
    try {
      // Looking at the xmlenc source, there should be no issue w/ wrapping
      // the stream -- i.e. no hanging resources.
      this.outputter = new XMLOutputter(this.out, new HbaseXMLEncoder());
      String os = System.getProperty("os.name").toLowerCase();
      // Shell likes the DOS output.
      this.outputter.setLineBreak(os.contains("windows")?
        LineBreak.DOS: LineBreak.UNIX);
      this.outputter.setIndentation(" ");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  

  /**
   * @param titles List of titles.  Pass null if no formatting (i.e.
   * no header, no footer, etc.
   * @throws IOException 
   */
  public void header(String[] titles) throws IOException {
    if (titles == null) {
      // print nothing.
      setNoFormatting(true);
      return;
    }
    // Can't add a 'border=1' attribute because its included on the end in
    
    this.outputter.startTag("table");
    this.outputter.startTag("tr");
    for (int i = 0; i < titles.length; i++) {
      this.outputter.startTag("th");
      this.outputter.pcdata(titles[i]);
      this.outputter.endTag();
    }
    this.outputter.endTag();
  }

  public void row(String [] cells) throws IOException{
    if (isNoFormatting()) {
      getOut().write(cells[0]);
      return;
    }
    this.outputter.startTag("tr");
    for (int i = 0; i < cells.length; i++) {
      this.outputter.startTag("td");
      this.outputter.pcdata(cells[i]);
      this.outputter.endTag();
    }
    this.outputter.endTag();
  }

  public void footer() throws IOException {
    if (!isNoFormatting()) {
      // To close the table
      this.outputter.endTag();
      this.outputter.endDocument();
    }
    // We're done. Clear flag.
    this.setNoFormatting(false);
    // If no formatting, output a newline to delimit cell and the
    // result summary output at end of every command.  If html, also emit a
    // newline to delimit html and summary line.
    getOut().write(System.getProperty("line.separator"));
    getOut().flush();
  }

  public Writer getOut() {
    return this.out;
  }
  
  public boolean isNoFormatting() {
    return this.noFormatting;
  }

  public void setNoFormatting(boolean noFormatting) {
    this.noFormatting = noFormatting;
  }
  
  public static void main(String[] args) throws IOException {
    HtmlTableFormatter f =
      new HtmlTableFormatter(new OutputStreamWriter(System.out, "UTF-8"));
    f.header(new String [] {"a", "b"});
    f.row(new String [] {"a", "b"});
    f.footer();
  }
}