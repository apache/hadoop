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

package org.apache.hadoop.mapreduce.lib.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * An InputFormat capable of performing joins over a set of data sources sorted
 * and partitioned the same way.
 *
 * A user may define new join types by setting the property
 * <tt>mapreduce.join.define.&lt;ident&gt;</tt> to a classname. 
 * In the expression <tt>mapreduce.join.expr</tt>, the identifier will be
 * assumed to be a ComposableRecordReader.
 * <tt>mapreduce.join.keycomparator</tt> can be a classname used to compare 
 * keys in the join.
 * @see #setFormat
 * @see JoinRecordReader
 * @see MultiFilterRecordReader
 */
@SuppressWarnings("unchecked")
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CompositeInputFormat<K extends WritableComparable>
    extends InputFormat<K, TupleWritable> {

  public static final String JOIN_EXPR = "mapreduce.join.expr";
  public static final String JOIN_COMPARATOR = "mapreduce.join.keycomparator";
  
  // expression parse tree to which IF requests are proxied
  private Parser.Node root;

  public CompositeInputFormat() { }


  /**
   * Interpret a given string as a composite expression.
   * {@code
   *   func  ::= <ident>([<func>,]*<func>)
   *   func  ::= tbl(<class>,"<path>")
   *   class ::= @see java.lang.Class#forName(java.lang.String)
   *   path  ::= @see org.apache.hadoop.fs.Path#Path(java.lang.String)
   * }
   * Reads expression from the <tt>mapreduce.join.expr</tt> property and
   * user-supplied join types from <tt>mapreduce.join.define.&lt;ident&gt;</tt>
   *  types. Paths supplied to <tt>tbl</tt> are given as input paths to the
   * InputFormat class listed.
   * @see #compose(java.lang.String, java.lang.Class, java.lang.String...)
   */
  public void setFormat(Configuration conf) throws IOException {
    addDefaults();
    addUserIdentifiers(conf);
    root = Parser.parse(conf.get(JOIN_EXPR, null), conf);
  }

  /**
   * Adds the default set of identifiers to the parser.
   */
  protected void addDefaults() {
    try {
      Parser.CNode.addIdentifier("inner", InnerJoinRecordReader.class);
      Parser.CNode.addIdentifier("outer", OuterJoinRecordReader.class);
      Parser.CNode.addIdentifier("override", OverrideRecordReader.class);
      Parser.WNode.addIdentifier("tbl", WrappedRecordReader.class);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("FATAL: Failed to init defaults", e);
    }
  }

  /**
   * Inform the parser of user-defined types.
   */
  private void addUserIdentifiers(Configuration conf) throws IOException {
    Pattern x = Pattern.compile("^mapreduce\\.join\\.define\\.(\\w+)$");
    for (Map.Entry<String,String> kv : conf) {
      Matcher m = x.matcher(kv.getKey());
      if (m.matches()) {
        try {
          Parser.CNode.addIdentifier(m.group(1),
              conf.getClass(m.group(0), null, ComposableRecordReader.class));
        } catch (NoSuchMethodException e) {
          throw new IOException("Invalid define for " + m.group(1), e);
        }
      }
    }
  }

  /**
   * Build a CompositeInputSplit from the child InputFormats by assigning the
   * ith split from each child to the ith composite split.
   */
  @SuppressWarnings("unchecked")
  public List<InputSplit> getSplits(JobContext job) 
      throws IOException, InterruptedException {
    setFormat(job.getConfiguration());
    job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.minsize", Long.MAX_VALUE);
    return root.getSplits(job);
  }

  /**
   * Construct a CompositeRecordReader for the children of this InputFormat
   * as defined in the init expression.
   * The outermost join need only be composable, not necessarily a composite.
   * Mandating TupleWritable isn't strictly correct.
   */
  @SuppressWarnings("unchecked") // child types unknown
  public RecordReader<K,TupleWritable> createRecordReader(InputSplit split, 
      TaskAttemptContext taskContext) 
      throws IOException, InterruptedException {
    setFormat(taskContext.getConfiguration());
    return root.createRecordReader(split, taskContext);
  }

  /**
   * Convenience method for constructing composite formats.
   * Given InputFormat class (inf), path (p) return:
   * {@code tbl(<inf>, <p>) }
   */
  public static String compose(Class<? extends InputFormat> inf, 
      String path) {
    return compose(inf.getName().intern(), path, 
             new StringBuffer()).toString();
  }

  /**
   * Convenience method for constructing composite formats.
   * Given operation (op), Object class (inf), set of paths (p) return:
   * {@code <op>(tbl(<inf>,<p1>),tbl(<inf>,<p2>),...,tbl(<inf>,<pn>)) }
   */
  public static String compose(String op, 
      Class<? extends InputFormat> inf, String... path) {
    final String infname = inf.getName();
    StringBuffer ret = new StringBuffer(op + '(');
    for (String p : path) {
      compose(infname, p, ret);
      ret.append(',');
    }
    ret.setCharAt(ret.length() - 1, ')');
    return ret.toString();
  }

  /**
   * Convenience method for constructing composite formats.
   * Given operation (op), Object class (inf), set of paths (p) return:
   * {@code <op>(tbl(<inf>,<p1>),tbl(<inf>,<p2>),...,tbl(<inf>,<pn>)) }
   */
  public static String compose(String op, 
      Class<? extends InputFormat> inf, Path... path) {
    ArrayList<String> tmp = new ArrayList<String>(path.length);
    for (Path p : path) {
      tmp.add(p.toString());
    }
    return compose(op, inf, tmp.toArray(new String[0]));
  }

  private static StringBuffer compose(String inf, String path,
      StringBuffer sb) {
    sb.append("tbl(" + inf + ",\"");
    sb.append(path);
    sb.append("\")");
    return sb;
  }
}
