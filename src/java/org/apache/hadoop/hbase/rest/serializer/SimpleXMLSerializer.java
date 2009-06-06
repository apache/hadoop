/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase.rest.serializer;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.rest.DatabaseModel.DatabaseMetadata;
import org.apache.hadoop.hbase.rest.Status.StatusMessage;
import org.apache.hadoop.hbase.rest.TableModel.Regions;
import org.apache.hadoop.hbase.rest.descriptors.RestCell;
import org.apache.hadoop.hbase.rest.descriptors.ScannerIdentifier;
import org.apache.hadoop.hbase.rest.descriptors.TimestampsDescriptor;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * Basic first pass at implementing an XML serializer for the REST interface.
 * This should probably be refactored into something better.
 * 
 */
public class SimpleXMLSerializer extends AbstractRestSerializer {

  private final AbstractPrinter printer;

  /**
   * @param response
   * @throws HBaseRestException
   */
  @SuppressWarnings("synthetic-access")
  public SimpleXMLSerializer(HttpServletResponse response)
      throws HBaseRestException {
    super(response, false);
    printer = new SimplePrinter(response);
  }

  @SuppressWarnings("synthetic-access")
  public SimpleXMLSerializer(HttpServletResponse response, boolean prettyPrint)
      throws HBaseRestException {
    super(response, prettyPrint);
    if (prettyPrint) {
      printer = new PrettyPrinter(response);
    } else {
      printer = new SimplePrinter(response);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.serializer.IRestSerializer#writeOutput(java
   * .lang.Object, java.io.OutputStream)
   */
  public void writeOutput(Object o) throws HBaseRestException {
    response.setContentType("text/xml");
    response.setCharacterEncoding(HConstants.UTF8_ENCODING);

    if (o instanceof ISerializable) {
      ((ISerializable) o).restSerialize(this);
    } else if (o.getClass().isArray()
        && o.getClass().getComponentType() == RowResult.class) {
      this.serializeRowResultArray((RowResult[]) o);
    } else if (o.getClass().isArray()
        && o.getClass().getComponentType() == Cell.class) {
      this.serializeCellArray((Cell[]) o);
    } else {
      throw new HBaseRestException(
          "Object does not conform to the ISerializable "
              + "interface.  Unable to generate xml output.");
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @seeorg.apache.hadoop.hbase.rest.serializer.IRestSerializer#
   * serializeDatabaseMetadata
   * (org.apache.hadoop.hbase.rest.DatabaseModel.DatabaseMetadata)
   */
  public void serializeDatabaseMetadata(DatabaseMetadata databaseMetadata)
      throws HBaseRestException {
    printer.print("<tables>");
    for (HTableDescriptor table : databaseMetadata.getTables()) {
      table.restSerialize(this);
    }
    printer.print("</tables>");
    printer.flush();
  }

  /*
   * (non-Javadoc)
   * 
   * @seeorg.apache.hadoop.hbase.rest.serializer.IRestSerializer#
   * serializeTableDescriptor(org.apache.hadoop.hbase.HTableDescriptor)
   */
  public void serializeTableDescriptor(HTableDescriptor tableDescriptor)
      throws HBaseRestException {
    printer.print("<table>");
    // name element
    printer.print("<name>");
    printer.print(tableDescriptor.getNameAsString());
    printer.print("</name>");
    // column families
    printer.print("<columnfamilies>");
    for (HColumnDescriptor column : tableDescriptor.getColumnFamilies()) {
      column.restSerialize(this);
    }
    printer.print("</columnfamilies>");
    printer.print("</table>");
    printer.flush();

  }

  /*
   * (non-Javadoc)
   * 
   * @seeorg.apache.hadoop.hbase.rest.serializer.IRestSerializer#
   * serializeColumnDescriptor(org.apache.hadoop.hbase.HColumnDescriptor)
   */
  public void serializeColumnDescriptor(HColumnDescriptor column)
      throws HBaseRestException {

    printer.print("<columnfamily>");
    // name
    printer.print("<name>");
    printer.print(org.apache.hadoop.hbase.util.Base64.encodeBytes(column.getName()));
    printer.print("</name>");
    // compression
    printer.print("<compression>");
    printer.print(column.getCompression().toString());
    printer.print("</compression>");
    // bloomfilter
    printer.print("<bloomfilter>");
    printer.print(column.getCompressionType().toString());
    printer.print("</bloomfilter>");
    // max-versions
    printer.print("<max-versions>");
    printer.print(column.getMaxVersions());
    printer.print("</max-versions>");
    printer.print("</columnfamily>");
    printer.flush();
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.serializer.IRestSerializer#serializeRegionData
   * (org.apache.hadoop.hbase.rest.TableModel.Regions)
   */
  public void serializeRegionData(Regions regions) throws HBaseRestException {

    printer.print("<regions>");
    for (byte[] region : regions.getRegionKey()) {
      printer.print("<region>");
      printer.print(Bytes.toString(region));
      printer.print("</region>");
    }
    printer.print("</regions>");
    printer.flush();
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.serializer.IRestSerializer#serializeStatusMessage
   * (org.apache.hadoop.hbase.rest.Status.StatusMessage)
   */
  public void serializeStatusMessage(StatusMessage message)
      throws HBaseRestException {

    printer.print("<status>");
    printer.print("<code>");
    printer.print(message.getStatusCode());
    printer.print("</code>");
    printer.print("<message>");
    printer.print(message.getMessage().toString());
    printer.print("</message>");
    printer.print("<error>");
    printer.print(message.getError());
    printer.print("</error>");
    printer.print("</status>");
    printer.flush();

  }

  /*
   * (non-Javadoc)
   * 
   * @seeorg.apache.hadoop.hbase.rest.serializer.IRestSerializer#
   * serializeScannerIdentifier(org.apache.hadoop.hbase.rest.ScannerIdentifier)
   */
  public void serializeScannerIdentifier(ScannerIdentifier scannerIdentifier)
      throws HBaseRestException {

    printer.print("<scanner>");
    printer.print("<id>");
    printer.print(scannerIdentifier.getId());
    printer.print("</id>");
    printer.print("</scanner>");
    printer.flush();
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.serializer.IRestSerializer#serializeRowResult
   * (org.apache.hadoop.hbase.io.RowResult)
   */
  public void serializeRowResult(RowResult rowResult) throws HBaseRestException {

    printer.print("<row>");
    printer.print("<name>");
    printer.print(org.apache.hadoop.hbase.util.Base64.encodeBytes(rowResult
        .getRow()));
    printer.print("</name>");
    printer.print("<columns>");
    for (RestCell cell : rowResult.getCells()) {
      printer.print("<column>");
      printer.print("<name>");
      printer.print(org.apache.hadoop.hbase.util.Base64.encodeBytes(cell
          .getName()));
      printer.print("</name>");
      printer.print("<timestamp>");
      printer.print(cell.getTimestamp());
      printer.print("</timestamp>");
      printer.print("<value>");
      printer.print(org.apache.hadoop.hbase.util.Base64.encodeBytes(cell
          .getValue()));
      printer.print("</value>");
      printer.print("</column>");
      printer.flush();
    }
    printer.print("</columns>");
    printer.print("</row>");
    printer.flush();
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.serializer.IRestSerializer#serializeRowResultArray
   * (org.apache.hadoop.hbase.io.RowResult[])
   */
  public void serializeRowResultArray(RowResult[] rows)
      throws HBaseRestException {
    printer.print("<rows>");
    for (RowResult row : rows) {
      row.restSerialize(this);
    }
    printer.print("</rows>");
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.serializer.IRestSerializer#serializeCell(org
   * .apache.hadoop.hbase.io.Cell)
   */
  public void serializeCell(Cell cell) throws HBaseRestException {
    printer.print("<cell>");
    printer.print("<value>");
    printer.print(org.apache.hadoop.hbase.util.Base64.encodeBytes(cell
        .getValue()));
    printer.print("</value>");
    printer.print("<timestamp>");
    printer.print(cell.getTimestamp());
    printer.print("</timestamp>");
    printer.print("</cell>");
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.serializer.IRestSerializer#serializeCellArray
   * (org.apache.hadoop.hbase.io.Cell[])
   */
  public void serializeCellArray(Cell[] cells) throws HBaseRestException {
    printer.print("<cells>");
    for (Cell cell : cells) {
      cell.restSerialize(this);
    }
    printer.print("</cells>");
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.rest.serializer.IRestSerializer#serializeTimestamps
   * (org.apache.hadoop.hbase.rest.RowModel.TimestampsDescriptor)
   */
  public void serializeTimestamps(TimestampsDescriptor timestampsDescriptor)
      throws HBaseRestException {
    // TODO Auto-generated method stub

  }

  // Private classes used for printing the output

  private interface IPrinter {
    public void print(String output);

    public void print(int output);

    public void print(long output);
    
    public void print(boolean output); 

    public void flush();
  }

  private abstract class AbstractPrinter implements IPrinter {
    protected final PrintWriter writer;

    @SuppressWarnings("unused")
    private AbstractPrinter() {
      writer = null;
    }

    public AbstractPrinter(HttpServletResponse response)
        throws HBaseRestException {
      try {
        writer = response.getWriter();
      } catch (IOException e) {
        throw new HBaseRestException(e.getMessage(), e);
      }
    }

    public void flush() {
      writer.flush();
    }
  }

  private class SimplePrinter extends AbstractPrinter {
    private SimplePrinter(HttpServletResponse response)
        throws HBaseRestException {
      super(response);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.hbase.rest.serializer.SimpleXMLSerializer.Printer#print
     * (java.io.PrintWriter, java.lang.String)
     */
    public void print(final String output) {
      writer.print(output);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.hbase.rest.serializer.SimpleXMLSerializer.IPrinter#
     * print(int)
     */
    public void print(int output) {
      writer.print(output);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.hbase.rest.serializer.SimpleXMLSerializer.IPrinter#
     * print(long)
     */
    public void print(long output) {
      writer.print(output);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.hbase.rest.serializer.SimpleXMLSerializer.IPrinter#print(boolean)
     */
    public void print(boolean output) {
      writer.print(output); 
    }
  }

  private class PrettyPrinter extends AbstractPrinter {
    private PrettyPrinter(HttpServletResponse response)
        throws HBaseRestException {
      super(response);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.hbase.rest.serializer.SimpleXMLSerializer.Printer#print
     * (java.io.PrintWriter, java.lang.String)
     */
    public void print(String output) {
      writer.println(output);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.hbase.rest.serializer.SimpleXMLSerializer.IPrinter#
     * print(int)
     */
    public void print(int output) {
      writer.println(output);

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.hbase.rest.serializer.SimpleXMLSerializer.IPrinter#
     * print(long)
     */
    public void print(long output) {
      writer.println(output);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.hbase.rest.serializer.SimpleXMLSerializer.IPrinter#print(boolean)
     */
    public void print(boolean output) {
      writer.println(output);
    }
  }
}
