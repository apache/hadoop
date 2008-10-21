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

package org.apache.hadoop.hive.ql.exec;


import java.io.*;
import java.net.URI;
import java.util.*;
import java.beans.*;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.plan.PlanUtils.ExpressionTypes;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

@SuppressWarnings("nls")
public class Utilities {

  /**
   * The object in the reducer are composed of these top level fields
   */

  public static enum ReduceField { KEY, VALUE, ALIAS };
  private static volatile mapredWork gWork = null;
  static final private Log LOG = LogFactory.getLog("hive.ql.exec.Utilities");

  public static void clearMapRedWork (Configuration job) {
    try {
      Path planPath = new Path(HiveConf.getVar(job, HiveConf.ConfVars.PLAN));
      FileSystem fs = FileSystem.get(job);
      if(fs.exists(planPath)) {
           try {
             fs.delete(planPath, true);
           } catch (IOException e) {
             e.printStackTrace();
           }
      }
    } catch (Exception e) {
    } finally {
      // where a single process works with multiple plans - we must clear
      // the cache before working with the next plan.
      gWork = null;
    }
  }

  public static mapredWork getMapRedWork (Configuration job) {
    try {
      if(gWork == null) {
        synchronized (Utilities.class) {
          if(gWork != null)
            return (gWork);
          InputStream in = new FileInputStream("HIVE_PLAN");
          mapredWork ret = deserializeMapRedWork(in);
          gWork = ret;
        }
        gWork.initialize();
      }
      return (gWork);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException (e);
    }
  }

  public static List<String> getFieldSchemaString(List<FieldSchema> fl) {
    if (fl == null) {
      return null;
    }
    
    ArrayList<String> ret = new ArrayList<String>();
    for(FieldSchema f: fl) {
      ret.add(f.getName() + " " + f.getType() + 
              (f.getComment() != null ? (" " + f.getComment()) : ""));
    }
    return ret;
  }
  
  /**
   * Java 1.5 workaround. 
   * From http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=5015403
   */
  public static class EnumDelegate extends DefaultPersistenceDelegate {
    @Override
      protected Expression instantiate(Object oldInstance, Encoder out) {
      return new Expression(Enum.class,
                            "valueOf",
                            new Object[] { oldInstance.getClass(), ((Enum<?>) oldInstance).name() });
    }
    protected boolean mutatesTo(Object oldInstance, Object newInstance) {
      return oldInstance == newInstance;
    }
  }

  public static void setMapRedWork (Configuration job, mapredWork w) {
    try {
      // use the default file system of the job
      FileSystem fs = FileSystem.get(job);
      Path planPath = new Path(HiveConf.getVar(job, HiveConf.ConfVars.SCRATCHDIR),
                               "plan."+randGen.nextInt());
      FSDataOutputStream out = fs.create(planPath);
      serializeMapRedWork(w, out);
      HiveConf.setVar(job, HiveConf.ConfVars.PLAN, planPath.toString());
      // Set up distributed cache
      DistributedCache.createSymlink(job);
      String uriWithLink = planPath.toUri().toString() + "#HIVE_PLAN";
      URI[] fileURIs = new URI[] {new URI(uriWithLink)};
      DistributedCache.setCacheFiles(fileURIs, job);
      // Cache the object in this process too so lookups don't hit the file system
      synchronized (Utilities.class) {
        gWork = w;
        gWork.initialize();
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException (e);
    }
  }

  public static void serializeTasks(Task<? extends Serializable> t, OutputStream out) {
    XMLEncoder e = new XMLEncoder(out);
    // workaround for java 1.5
    e.setPersistenceDelegate( ExpressionTypes.class, new EnumDelegate() );
    e.setPersistenceDelegate( groupByDesc.Mode.class, new EnumDelegate());
    e.writeObject(t);
    e.close();
  }

  /**
   * Serialize the plan object to an output stream.
   * DO NOT use this to write to standard output since it closes the output stream
   * DO USE mapredWork.toXML() instead
   */
  public static void serializeMapRedWork(mapredWork w, OutputStream out) {
    XMLEncoder e = new XMLEncoder(out);
    // workaround for java 1.5
    e.setPersistenceDelegate( ExpressionTypes.class, new EnumDelegate() );
    e.setPersistenceDelegate( groupByDesc.Mode.class, new EnumDelegate());
    e.writeObject(w);
    e.close();
  }

  public static mapredWork deserializeMapRedWork (InputStream in) {
    XMLDecoder d = new XMLDecoder(in);
    mapredWork ret = (mapredWork)d.readObject();
    d.close();
    return (ret);
  }

  public static class Tuple<T, V> {
    private T one;
    private V two;

    public Tuple(T one, V two) {
      this.one = one;
      this.two = two;
    }
    public T getOne() {return this.one;}
    public V getTwo() {return this.two;}
  }

  public static tableDesc defaultTd;
  static {
    // by default we expect ^A separated strings
    defaultTd = PlanUtils.getDefaultTableDesc("" + Utilities.ctrlaCode);
  }

  public static tableDesc defaultTabTd;
  static {
    // Default tab-separated tableDesc
    defaultTabTd = PlanUtils.getDefaultTableDesc("" + Utilities.tabCode);
  }
  
  public final static int newLineCode = 10;
  public final static int tabCode = 9;
  public final static int ctrlaCode = 1;
  public final static ByteWritable zeroByteWritable = new ByteWritable (0);

  // Note: When DDL supports specifying what string to represent null,
  // we should specify "NULL" to represent null in the temp table, and then 
  // we can make the following translation deprecated.  
  public static String nullStringStorage = "\\N";
  public static String nullStringOutput = "NULL";

  public static Random randGen = new Random();
  
  /**
   * Gets the task id if we are running as a Hadoop job.
   * Gets a random number otherwise.
   */ 
  public static String getTaskId(Configuration hconf) {
    String taskid = (hconf == null) ? null : hconf.get("mapred.task.id");
    if((taskid == null) || taskid.equals("")) {
      return (""+randGen.nextInt());
    } else {
      return taskid.replaceAll("task_[0-9]+_", "");
    }
  }

  public static HashMap makeMap(Object ... olist) {
    HashMap ret = new HashMap ();
    for(int i=0; i<olist.length; i += 2) {
      ret.put(olist[i], olist[i+1]);
    }
    return (ret);
  }

  public static Properties makeProperties(String ... olist) {
    Properties ret = new Properties ();
    for(int i=0; i<olist.length; i += 2) {
      ret.setProperty(olist[i], olist[i+1]);
    }
    return (ret);
  }

  public static ArrayList makeList(Object ... olist) {
    ArrayList ret = new ArrayList ();
    for(int i=0; i<olist.length; i++) {
      ret.add(olist[i]);
    }
    return (ret);
  }



  public static class StreamPrinter extends Thread {
    InputStream is;
    String type;
    PrintStream os;
    
    public StreamPrinter(InputStream is, String type, PrintStream os) {
      this.is = is;
      this.type = type;
      this.os = os;
    }
    
    public void run() {
      try {
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        String line=null;
        if(type != null) {
          while ( (line = br.readLine()) != null)
            os.println(type + ">" + line);
        } else {
          while ( (line = br.readLine()) != null)
            os.println(line);
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();  
      }
    }
  }

  public static tableDesc getTableDesc(Table tbl) {
    return (new tableDesc (tbl.getDeserializer().getClass(), tbl.getInputFormatClass(), tbl.getOutputFormatClass(), tbl.getSchema()));
  }


  public static partitionDesc getPartitionDesc(Partition part) {
    return (new partitionDesc (getTableDesc(part.getTable()), part.getSpec()));
  }

  public static void addMapWork(mapredWork mr, Table tbl, String alias, Operator<?> work) {
    mr.addMapWork(tbl.getDataLocation().getPath(), alias, work, 
                  new partitionDesc(getTableDesc(tbl), null));
  }

  private static String getOpTreeSkel_helper(Operator<?> op, String indent) {
    if (op == null)
      return "";
  
    StringBuffer sb = new StringBuffer();
    sb.append(indent);
    sb.append(op.toString());
    sb.append("\n");
    if (op.getChildOperators() != null)
      for(Object child: op.getChildOperators()) {
        sb.append(getOpTreeSkel_helper((Operator<?>)child, indent + "  "));
      }

    return sb.toString();
  }

  public static String getOpTreeSkel(Operator<?> op) {
    return getOpTreeSkel_helper(op, "");
  }

  private static boolean isWhitespace( int c ) {
    if( c == -1 ) { return false; }
    return Character.isWhitespace( ( char )c );
  }

  public static boolean contentsEqual( InputStream is1, InputStream is2, boolean ignoreWhitespace )
    throws IOException {
    try {
      if((is1 == is2) || (is1 == null && is2 == null))
          return true;
 
      if(is1 == null || is2 == null)
        return false;
 
      while( true ) {
        int c1 = is1.read();
        while( ignoreWhitespace && isWhitespace( c1 ) )
          c1 = is1.read();
        int c2 = is2.read();
        while( ignoreWhitespace && isWhitespace( c2 ) )
          c2 = is2.read();
        if( c1 == -1 && c2 == -1 )
          return true;
        if( c1 != c2 )
          break;
      }
    } catch( FileNotFoundException e ) {
      e.printStackTrace();
    }
    return false;
  }


  /**
   * convert "From src insert blah blah" to "From src insert ... blah"
   */
  public static String abbreviate(String str, int max) {
    str = str.trim();

    int len = str.length();
    int suffixlength = 20;

    if(len <= max)
      return str;


    suffixlength = Math.min(suffixlength, (max-3)/2);
    String rev = StringUtils.reverse(str);
    
    // get the last few words 
    String suffix = WordUtils.abbreviate(rev, 0, suffixlength, "");
    suffix = StringUtils.reverse(suffix);

    // first few ..
    String prefix = StringUtils.abbreviate(str, max-suffix.length());

    return prefix+suffix;
  }

  public final static String NSTR = "";
  public static enum streamStatus {EOF, TERMINATED}
  public static streamStatus readColumn(DataInput in, OutputStream out) throws IOException {

    while (true) {
      int b;
      try {
        b = (int)in.readByte();
      } catch (EOFException e) {
        return streamStatus.EOF;
      }

      if (b == Utilities.newLineCode) {
        return streamStatus.TERMINATED;
      }

      out.write(b);
    }
    // Unreachable
  }
  
  public static OutputStream createCompressedStream(JobConf jc,
                                                    OutputStream out) throws IOException {
    boolean isCompressed = FileOutputFormat.getCompressOutput(jc);
    if(isCompressed) {
      Class<? extends CompressionCodec> codecClass =
        FileOutputFormat.getOutputCompressorClass(jc, DefaultCodec.class);
      CompressionCodec codec = (CompressionCodec)
        ReflectionUtils.newInstance(codecClass, jc);
      return codec.createOutputStream(out);
    } else {
      return (out);
    }
  }

  public static SequenceFile.Writer createSequenceWriter(JobConf jc, FileSystem fs,
                                                         Path file, Class<?> keyClass,
                                                         Class<?> valClass) throws IOException {
    CompressionCodec codec = null;
    CompressionType compressionType = CompressionType.NONE;
    if (SequenceFileOutputFormat.getCompressOutput(jc)) {
      compressionType = SequenceFileOutputFormat.getOutputCompressionType(jc);
      Class codecClass = SequenceFileOutputFormat.getOutputCompressorClass(jc, DefaultCodec.class);
      codec = (CompressionCodec) 
        ReflectionUtils.newInstance(codecClass, jc);
    }
    return (SequenceFile.createWriter(fs, jc, file,
                                      keyClass, valClass, compressionType, codec));

  }
}
