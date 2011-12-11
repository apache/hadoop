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

package org.apache.hadoop.vertica;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

/**
 * Serializable record for records returned from and written to Vertica
 * 
 */
public class VerticaRecord implements Writable {
  ResultSet results = null;
  ResultSetMetaData meta = null;
  int columns = 0;
  List<Integer> types = null;
  List<Object> values = null;
  List<String> names = null;
  boolean dateString;
  String delimiter = VerticaConfiguration.DELIMITER;
  String terminator = VerticaConfiguration.RECORD_TERMINATER;

  DateFormat datefmt = new SimpleDateFormat("yyyyMMdd");
  DateFormat timefmt = new SimpleDateFormat("HHmmss");
  DateFormat tmstmpfmt = new SimpleDateFormat("yyyyMMddHHmmss");
  DateFormat sqlfmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public List<Object> getValues() {
    return values;
  }

  public List<Integer> getTypes() {
    return types;
  }

  /**
   * Create a new VerticaRecord class out of a query result set
   * 
   * @param results
   *          ResultSet returned from running input split query
   * @param dateString
   *          True if dates should be marshaled as strings
   * @throws SQLException
   */
  VerticaRecord(ResultSet results, boolean dateString) throws SQLException {
    this.results = results;
    this.dateString = dateString;
    meta = results.getMetaData();
    columns = meta.getColumnCount();
    names = new ArrayList<String>(columns);
    types = new ArrayList<Integer>(columns);
    values = new ArrayList<Object>(columns);
    for (int i = 0; i < columns; i++) {
      names.add(meta.getCatalogName(i + 1));
      types.add(meta.getColumnType(i + 1));
      values.add(null);
    }
  }

  public VerticaRecord() {
    this.types = new ArrayList<Integer>();
    this.values = new ArrayList<Object>();
  }

  public VerticaRecord(List<String> names, List<Integer> types) {
    this.names = names;
    this.types = types;
    values = new ArrayList<Object>();
    for (@SuppressWarnings("unused")
    Integer type : types)
      values.add(null);
    columns = values.size();
  }

  public VerticaRecord(List<Object> values, boolean parseTypes) {
    this.types = new ArrayList<Integer>();
    this.values = values;
    columns = values.size();
    objectTypes();
  }

  /**
   * Test interface for junit tests that do not require a database
   * 
   * @param types
   * @param values
   * @param dateString
   */
  public VerticaRecord(List<String> names, List<Integer> types,
      List<Object> values, boolean dateString) {
    this.names = names;
    this.types = types;
    this.values = values;
    this.dateString = dateString;
    columns = types.size();
    if (types.size() == 0)
      objectTypes();
  }

  public Object get(String name) throws Exception {
    if (names == null || names.size() == 0)
      throw new Exception("Cannot set record by name if names not initialized");
    int i = names.indexOf(name);
    return get(i);
  }

  public Object get(int i) {
    if (i >= values.size())
      throw new IndexOutOfBoundsException("Index " + i
          + " greater than input size " + values.size());
    return values.get(i);
  }

  public void set(String name, Object value) throws Exception {
    if (names == null || names.size() == 0)
      throw new Exception("Cannot set record by name if names not initialized");
    int i = names.indexOf(name);
    set(i, value);
  }

  /**
   * set a value, 0 indexed
   * 
   * @param i
   */
  public void set(Integer i, Object value) {
    set(i, value, false);
  }

  /**
   * set a value, 0 indexed
   * 
   * @param i
   */
  public void set(Integer i, Object value, boolean validate) {
    if (i >= values.size())
      throw new IndexOutOfBoundsException("Index " + i
          + " greater than input size " + values.size());
    if (validate && value != null) {
      Integer type = types.get(i);
      switch (type) {
      case Types.BIGINT:
        if (!(value instanceof Long) && !(value instanceof Integer)
            && !(value instanceof Short) && !(value instanceof LongWritable)
            && !(value instanceof VLongWritable)
            && !(value instanceof VIntWritable))
          throw new ClassCastException("Cannot cast "
              + value.getClass().getName() + " to Long");
        break;
      case Types.INTEGER:
        if (!(value instanceof Integer) && !(value instanceof Short)
            && !(value instanceof VIntWritable))
          throw new ClassCastException("Cannot cast "
              + value.getClass().getName() + " to Integer");
        break;
      case Types.TINYINT:
      case Types.SMALLINT:
        if (!(value instanceof Short))
          throw new ClassCastException("Cannot cast "
              + value.getClass().getName() + " to Short");
        break;
      case Types.REAL:
      case Types.DECIMAL:
      case Types.NUMERIC:
        if (!(value instanceof BigDecimal))
          throw new ClassCastException("Cannot cast "
              + value.getClass().getName() + " to BigDecimal");
      case Types.DOUBLE:
        if (!(value instanceof Double) && !(value instanceof Float)
            && !(value instanceof DoubleWritable)
            && !(value instanceof FloatWritable))
          throw new ClassCastException("Cannot cast "
              + value.getClass().getName() + " to Double");
        break;
      case Types.FLOAT:
        if (!(value instanceof Float) && !(value instanceof FloatWritable))
          throw new ClassCastException("Cannot cast "
              + value.getClass().getName() + " to Float");
        break;
      case Types.BINARY:
      case Types.LONGVARBINARY:
      case Types.VARBINARY:
        if (!(value instanceof byte[]) && !(value instanceof BytesWritable))
          throw new ClassCastException("Cannot cast "
              + value.getClass().getName() + " to byte[]");
        break;
      case Types.BIT:
      case Types.BOOLEAN:
        if (!(value instanceof Boolean) && !(value instanceof BooleanWritable)
            && !(value instanceof ByteWritable))
          throw new ClassCastException("Cannot cast "
              + value.getClass().getName() + " to Boolean");
        break;
      case Types.CHAR:
        if (!(value instanceof Character) && !(value instanceof String))
          throw new ClassCastException("Cannot cast "
              + value.getClass().getName() + " to Character");
        break;
      case Types.LONGNVARCHAR:
      case Types.LONGVARCHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.VARCHAR:
        if (!(value instanceof String) && !(value instanceof Text))
          throw new ClassCastException("Cannot cast "
              + value.getClass().getName() + " to String");
        break;
      case Types.DATE:
        if (!(value instanceof Date) && !(value instanceof java.util.Date))
          throw new ClassCastException("Cannot cast "
              + value.getClass().getName() + " to Date");
        break;
      case Types.TIME:
        if (!(value instanceof Time) && !(value instanceof java.util.Date))
          throw new ClassCastException("Cannot cast "
              + value.getClass().getName() + " to Time");
        break;
      case Types.TIMESTAMP:
        if (!(value instanceof Timestamp) && !(value instanceof java.util.Date))
          throw new ClassCastException("Cannot cast "
              + value.getClass().getName() + " to Timestamp");
        break;
      default:
        throw new RuntimeException("Unknown type value " + types.get(i));
      }
    }
    values.set(i, value);
  }

  public boolean next() throws SQLException {
    if (results.next()) {
      for (int i = 1; i <= columns; i++)
        values.set(i - 1, results.getObject(i));
      return true;
    }
    return false;
  }

  private void objectTypes() {
    for (Object obj : values) {
      if (obj == null) {
        this.types.add(Types.NULL);
      } else if (obj instanceof Long) {
        this.types.add(Types.BIGINT);
      } else if (obj instanceof LongWritable) {
        this.types.add(Types.BIGINT);
      } else if (obj instanceof VLongWritable) {
        this.types.add(Types.BIGINT);
      } else if (obj instanceof VIntWritable) {
        this.types.add(Types.INTEGER);
      } else if (obj instanceof Integer) {
        this.types.add(Types.INTEGER);
      } else if (obj instanceof Short) {
        this.types.add(Types.SMALLINT);
      } else if (obj instanceof BigDecimal) {
        this.types.add(Types.NUMERIC);
      } else if (obj instanceof DoubleWritable) {
        this.types.add(Types.DOUBLE);
      } else if (obj instanceof Double) {
        this.types.add(Types.DOUBLE);
      } else if (obj instanceof Float) {
        this.types.add(Types.FLOAT);
      } else if (obj instanceof FloatWritable) {
        this.types.add(Types.FLOAT);
      } else if (obj instanceof byte[]) {
        this.types.add(Types.BINARY);
      } else if (obj instanceof ByteWritable) {
        this.types.add(Types.BINARY);
      } else if (obj instanceof Boolean) {
        this.types.add(Types.BOOLEAN);
      } else if (obj instanceof BooleanWritable) {
        this.types.add(Types.BOOLEAN);
      } else if (obj instanceof Character) {
        this.types.add(Types.CHAR);
      } else if (obj instanceof String) {
        this.types.add(Types.VARCHAR);
      } else if (obj instanceof BytesWritable) {
        this.types.add(Types.VARCHAR);
      } else if (obj instanceof Text) {
        this.types.add(Types.VARCHAR);
      } else if (obj instanceof java.util.Date) {
        this.types.add(Types.DATE);
      } else if (obj instanceof Date) {
        this.types.add(Types.DATE);
      } else if (obj instanceof Time) {
        this.types.add(Types.TIME);
      } else if (obj instanceof Timestamp) {
        this.types.add(Types.TIMESTAMP);
      } else {
        throw new RuntimeException("Unknown type " + obj.getClass().getName()
            + " passed to Vertica Record");
      }
    }
  }

  public String toSQLString() {
    return toSQLString(delimiter, terminator);
  }

  public String toSQLString(String delimiterArg, String terminatorArg) {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < columns; i++) {
      Object obj = values.get(i);
      Integer type = types.get(i);

      // switch statement uses fall through to handle type variations
      // e.g. type specified as BIGINT but passed in as Integer
      switch (type) {
      case Types.NULL:
        sb.append("");
        break;
      case Types.BIGINT:
        if (obj instanceof Long) {
          sb.append(obj.toString());
          break;
        }
      case Types.INTEGER:
        if (obj instanceof Integer) {
          sb.append(obj.toString());
          break;
        }
      case Types.TINYINT:
      case Types.SMALLINT:
        if (obj instanceof Short) {
          sb.append(obj.toString());
          break;
        }
        if (obj instanceof LongWritable) {
          sb.append(((LongWritable) obj).get());
          break;
        }
        if (obj instanceof VLongWritable) {
          sb.append(((VLongWritable) obj).get());
          break;
        }
        if (obj instanceof VIntWritable) {
          sb.append(((VIntWritable) obj).get());
          break;
        }
      case Types.REAL:
      case Types.DECIMAL:
      case Types.NUMERIC:
        if (obj instanceof BigDecimal) {
          sb.append(obj.toString());
          break;
        }
      case Types.DOUBLE:
        if (obj instanceof Double) {
          sb.append(obj.toString());
          break;
        }
        if (obj instanceof DoubleWritable) {
          sb.append(((DoubleWritable) obj).get());
          break;
        }
      case Types.FLOAT:
        if (obj instanceof Float) {
          sb.append(obj.toString());
          break;
        }
        if (obj instanceof FloatWritable) {
          sb.append(((FloatWritable) obj).get());
          break;
        }
      case Types.BINARY:
      case Types.LONGVARBINARY:
      case Types.VARBINARY:
        if(obj == null) sb.append("");
        else sb.append(ByteBuffer.wrap((byte[]) obj).asCharBuffer());
        break;
      case Types.BIT:
      case Types.BOOLEAN:
        if (obj instanceof Boolean) {
          if ((Boolean) obj)
            sb.append("true");
          else
            sb.append("false");
          break;
        }
        if (obj instanceof BooleanWritable) {
          if (((BooleanWritable) obj).get())
            sb.append("true");
          else
            sb.append("false");
          break;
        }
      case Types.LONGNVARCHAR:
      case Types.LONGVARCHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.VARCHAR:
        if (obj instanceof String) {
          sb.append((String) obj);
          break;
        }
        if (obj instanceof byte[]) {
          sb.append((byte[]) obj);
          break;
        }
        if (obj instanceof BytesWritable) {
          sb.append(((BytesWritable) obj).getBytes());
          break;
        }
      case Types.CHAR:
        if (obj instanceof Character) {
          sb.append((Character) obj);
          break;
        }
        if (obj instanceof ByteWritable) {
          sb.append(((ByteWritable) obj).get());
          break;
        }
      case Types.DATE:
      case Types.TIME:
      case Types.TIMESTAMP:
        if (obj instanceof java.util.Date)
          sb.append(sqlfmt.format((java.util.Date) obj));
        else if (obj instanceof Date)
          sb.append(sqlfmt.format((Date) obj));
        else if (obj instanceof Time)
          sb.append(sqlfmt.format((Time) obj));
        else if (obj instanceof Timestamp)
          sb.append(sqlfmt.format((Timestamp) obj));
        break;
      default:
        if(obj == null) sb.append("");
        else throw new RuntimeException("Unknown type value " + types.get(i));
      }
      if (i < columns - 1)
        sb.append(delimiterArg);
      else
        sb.append(terminatorArg);
    }
    return sb.toString();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    columns = in.readInt();
    if (types.size() > 0)
      types.clear();
    for (int i = 0; i < columns; i++)
      types.add(in.readInt());

    for (int i = 0; i < columns; i++) {
      int type = types.get(i);
      switch (type) {
      case Types.NULL:
        values.add(null);
        break;
      case Types.BIGINT:
        values.add(in.readLong());
        break;
      case Types.INTEGER:
        values.add(in.readInt());
        break;
      case Types.TINYINT:
      case Types.SMALLINT:
        values.add(in.readShort());
        break;
      case Types.REAL:
      case Types.DECIMAL:
      case Types.NUMERIC:
        values.add(new BigDecimal(Text.readString(in)));
        break;
      case Types.DOUBLE:
        values.add(in.readDouble());
        break;
      case Types.FLOAT:
        values.add(in.readFloat());
        break;
      case Types.BINARY:
      case Types.LONGVARBINARY:
      case Types.VARBINARY:
        values.add(StringUtils.hexStringToByte(Text.readString(in)));
        break;
      case Types.BIT:
      case Types.BOOLEAN:
        values.add(in.readBoolean());
        break;
      case Types.CHAR:
        values.add(in.readChar());
        break;
      case Types.LONGNVARCHAR:
      case Types.LONGVARCHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.VARCHAR:
        values.add(Text.readString(in));
        break;
      case Types.DATE:
        if (dateString)
          try {
            values.add(new Date(datefmt.parse(Text.readString(in)).getTime()));
          } catch (ParseException e) {
            throw new IOException(e);
          }
        else
          values.add(new Date(in.readLong()));
        break;
      case Types.TIME:
        if (dateString)
          try {
            values.add(new Time(timefmt.parse(Text.readString(in)).getTime()));
          } catch (ParseException e) {
            throw new IOException(e);
          }
        else
          values.add(new Time(in.readLong()));
        break;
      case Types.TIMESTAMP:
        if (dateString)
          try {
            values.add(new Timestamp(tmstmpfmt.parse(Text.readString(in))
                .getTime()));
          } catch (ParseException e) {
            throw new IOException(e);
          }
        else
          values.add(new Timestamp(in.readLong()));
        break;
      default:
        throw new IOException("Unknown type value " + type);
      }
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(columns);
    
    for (int i = 0; i < columns; i++) {
      Object obj = values.get(i);
      Integer type = types.get(i);
      if(obj == null) out.writeInt(Types.NULL);
      else out.writeInt(type);
    }

    for (int i = 0; i < columns; i++) {
      Object obj = values.get(i);
      Integer type = types.get(i);

      if(obj == null) continue;
      
      switch (type) {
      case Types.BIGINT:
        out.writeLong((Long) obj);
        break;
      case Types.INTEGER:
        out.writeInt((Integer) obj);
        break;
      case Types.TINYINT:
      case Types.SMALLINT:
        out.writeShort((Short) obj);
        break;
      case Types.REAL:
      case Types.DECIMAL:
      case Types.NUMERIC:
        Text.writeString(out, obj.toString());
        break;
      case Types.DOUBLE:
        out.writeDouble((Double) obj);
        break;
      case Types.FLOAT:
        out.writeFloat((Float) obj);
        break;
      case Types.BINARY:
      case Types.LONGVARBINARY:
      case Types.VARBINARY:
        Text.writeString(out, StringUtils.byteToHexString((byte[]) obj));
        break;
      case Types.BIT:
      case Types.BOOLEAN:
        out.writeBoolean((Boolean) obj);
        break;
      case Types.CHAR:
        out.writeChar((Character) obj);
        break;
      case Types.LONGNVARCHAR:
      case Types.LONGVARCHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.VARCHAR:
        Text.writeString(out, (String) obj);
        break;
      case Types.DATE:
        if (obj instanceof java.util.Date) {
          if (dateString)
            Text.writeString(out, datefmt.format((java.util.Date) obj));
          else
            out.writeLong(((java.util.Date) obj).getTime());
        } else {
          if (dateString)
            Text.writeString(out, datefmt.format((Date) obj));
          else
            out.writeLong(((Date) obj).getTime());
        }
        break;
      case Types.TIME:
        if (dateString)
          Text.writeString(out, timefmt.format((Time) obj));
        else
          out.writeLong(((Time) obj).getTime());
        break;
      case Types.TIMESTAMP:
        if (dateString)
          Text.writeString(out, tmstmpfmt.format((Timestamp) obj));
        else
          out.writeLong(((Timestamp) obj).getTime());
        break;
      default:
        throw new IOException("Unknown type value " + types.get(i));
      }
    }
  }

}