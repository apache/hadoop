package org.apache.hadoop.hbase.shell.formatter;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.hadoop.hbase.shell.TableFormatter;
import org.znerd.xmlenc.LineBreak;
import org.znerd.xmlenc.XMLOutputter;

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
  
  public HtmlTableFormatter(final Writer o) {
    this.out = o;
    try {
      // Looking at the xmlenc source, there should be no issue w/ wrapping
      // the stream -- i.e. no hanging resources.
      this.outputter = new XMLOutputter(this.out, "UTF-8");
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