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

package org.apache.hadoop.streaming;

import java.io.File;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;

/** Parses a -input &lt;spec> that determines the DFS paths that will 
 be accessed by a MergedInputFormat.<br>
 CompoundDirSpec.getPaths() is a 2-D ragged array of DFS paths.<br>
 One of the paths is the <b>primary</b> and can contain a globbing pattern
 to match multiple files.<br>
 The other paths are <b>secondary</b> and must indicate either a directory or a single file.
 During execution secondary files are computed to be the secondary path 
 plus the primary non-qualified filename.
 Example: <tt>
 -input "/filter/colsx NULL | +/batch1/colsx/* /batch1/colsy/"
 -input "/filter/colsx NULL | +/batch2/colsx/* /batch2/colsy/"
 </tt>
 Files and contents:<tt>
 /filter/colsx/part-00000:
 /batch1/colsx/part-00000:
 /batch1/colsy/part-00000:
 /batch2/colsx/part-00000:
 /batch2/colsy/part-00000:
 </tt>
 Mapper input:<tt>
 </tt>
 Side-effect outputs with Identity "mapper":<tt>

 </tt>
 @author Michel Tourn
 */
class CompoundDirSpec {

  // Keep the Usage messages and docs in sync!
  public final static String MERGEGLOB_PREFIX = "||";
  public final static String MERGE_SEP = "|";
  public final static String COL_SEP = " ";
  public final static String PRIMARY_PREFIX = "+";

  CompoundDirSpec(String argSpec, boolean isInputSpec) {
    argSpec_ = argSpec;
    isInputSpec_ = isInputSpec;

    direction_ = isInputSpec_ ? "input" : "output";
    parse();
  }

  public void parse() throws IllegalStateException {
    String[] mergerSpecs = argSpec_.split(StreamUtil.regexpEscape(MERGE_SEP));

    int msup = mergerSpecs.length;
    paths_ = new String[msup][];

    if (msup == 0) {
      throw new IllegalStateException("A -" + direction_ + " spec needs at list one path");
    }
    if (false == isInputSpec_) {
      if (msup > 1) {
        throw new IllegalStateException("A -output spec cannot use merged streams ('" + MERGE_SEP
            + "' delimiter)");
      }
    }
    for (int m = 0; m < msup; m++) {
      String merged = mergerSpecs[m];
      merged = merged.trim();
      String[] colSpecs = merged.split(StreamUtil.regexpEscape(COL_SEP));
      int csup = colSpecs.length;
      if (csup == 0) {
        throw new IllegalStateException("A -input spec needs at list one path spec per |<column>|");
      }
      paths_[m] = new String[csup];
      for (int c = 0; c < csup; c++) {
        String spec = colSpecs[c];
        if (spec.startsWith(PRIMARY_PREFIX)) {
          // for (!isInputSpec_) the tuple paths should be symmetric.
          // but we still allow user to specify one in case setOutputDir makes a difference
          if (prow_ != NA) {
            throwBadNumPrimaryInputSpecs();
          }
          spec = spec.substring(PRIMARY_PREFIX.length());
          prow_ = m;
          pcol_ = c;
        }
        paths_[m][c] = spec;
      }
    }
    if (prow_ == NA) {
      if (!isInputSpec_) {
        // pick an 'arbitrary' one -- the tuple paths should be symmetric.
        prow_ = 0;
        pcol_ = 0;
      } else if (msup == 1 && paths_[0].length == 1) {
        // pick the only one available. That's also bw-compatible syntax
        prow_ = 0;
        pcol_ = 0;
      } else {
        throwBadNumPrimaryInputSpecs();
      }
    }
  }

  void throwBadNumPrimaryInputSpecs() throws IllegalStateException {
    String msg = "A compound -input spec needs exactly one primary path prefixed with "
        + PRIMARY_PREFIX;
    msg += ":\n";
    msg += toTableString();
    throw new IllegalStateException(msg);
  }

  // TBD need to decide early whether they are dirs or files or globs?
  public void validatePaths(FileSystem fs) {
    int rsup = paths_.length;
    for (int r = 0; r < rsup; r++) {
      int csup = paths_[r].length;
      for (int c = 0; c < csup; c++) {
        String path = paths_[r][c];
      }
    }
  }

  public int primaryRow() {
    return prow_;
  }

  public int primaryCol() {
    return pcol_;
  }

  public String primarySpec() {
    return paths_[prow_][pcol_];
  }
  
  /*
   Example input spec in table form:
   <1 +[/input/part-00] 
   <2  [/input/part-01] 
   <3  [/input/part-02] 
   Example output spec in table form:
   +[/my.output] 
   */
  public String toTableString() {
    StringBuffer buf = new StringBuffer();
    int maxWid = 0;
    for (int pass = 1; pass <= 2; pass++) {
      int rsup = paths_.length;
      for (int r = 0; r < rsup; r++) {
        int csup = paths_[r].length;
        for (int c = 0; c < csup; c++) {
          String cell = "[" + paths_[r][c] + "]";
          if (r == prow_ && c == pcol_) {
            cell = PRIMARY_PREFIX + cell;
          } else {
            cell = StreamUtil.rjustify(cell, cell.length() + PRIMARY_PREFIX.length());
          }
          if (isInputSpec_) {
            // channels are for tagged input streams: r-based
            if (rsup > 1) {
              String channel = "<" + (r + 1);
              cell = channel + " " + cell;
            }
          } else {
            // channels are for columns (multiple files) c-based
            if (csup > 1) {
              String channel = ">" + (c + 1);
              cell = channel + " " + cell;
            }
          }
          if (pass == 2) {
            cell = StreamUtil.ljustify(cell, maxWid);
            buf.append(cell);
            buf.append(" ");
          } else {
            if (cell.length() > maxWid) {
              maxWid = cell.length();
            }
          }
        }
        if (pass == 2) {
          buf.append("\n");
        }
      }
    }
    return buf.toString();
  }

  /** 
   @see #primaryRow 
   @see #primaryCol
   */
  public String[][] getPaths() {
    return paths_;
  }

  // ==== Static helpers that depend on a JobConf. ====
  
  // Unlike CompoundDirSpec.parse() which is reexecuted at Task runtime,
  // this is expanded once in advance and relies on client-side DFS access.
  // Main reason is we need to choose a primary input file at submission time. 
  public static String expandGlobInputSpec(String inputSpec, JobConf job)
  {
    inputSpec = inputSpec.trim();
    if(!inputSpec.startsWith(MERGEGLOB_PREFIX)) {
      return inputSpec;
    }
    inputSpec = inputSpec.substring(MERGEGLOB_PREFIX.length());
    // TODO use upcoming DFSShell wildcarding code..
    return inputSpec;
  }
  
  // find the -input statement that contains the job's split
  // TODO test with globbing / directory /single file
  public static CompoundDirSpec findInputSpecForPrimary(String primary, JobConf job) {
    int num = job.getInt("stream.numinputspecs", -1);
    for (int s = 0; s < num; s++) {
      String specStr = job.get("stream.inputspecs." + s);
      CompoundDirSpec spec = new CompoundDirSpec(specStr, true);
      if (pathsMatch(spec.primarySpec(), primary, job)) {
        return spec;
      }
    }
    return null;
  }

  // There can be only one output spec but this provides some server-side validation
  public static CompoundDirSpec findOutputSpecForPrimary(String primary, JobConf job) {
    String specStr = job.get("stream.outputspec");
    CompoundDirSpec spec = new CompoundDirSpec(specStr, false);
    if (pathsMatch(spec.primarySpec(), primary, job)) {
      return spec;
    }
    return spec;
  }

  static boolean pathsMatch(String s1, String s2, JobConf job) {
    boolean isLocalFS = job.get("fs.default.name", "").equals("local");
    if (isLocalFS) {
      s1 = StreamUtil.safeGetCanonicalPath(new File(s1));
      s2 = StreamUtil.safeGetCanonicalPath(new File(s2));
    }
    return (s1.equals(s2));
  }

  final static int NA = -1;

  String argSpec_;
  boolean isInputSpec_;

  String direction_;
  String[][] paths_;
  int prow_ = NA;
  int pcol_ = NA;
}
