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
 
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A class that allows a map/red job to work on a sample of sequence files.
 * The sample is decided by the filter class set by the job.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFileInputFilter<K, V>
  extends SequenceFileInputFormat<K, V> {
  
  final private static String FILTER_CLASS = org.apache.hadoop.mapreduce.lib.
      input.SequenceFileInputFilter.FILTER_CLASS;

  public SequenceFileInputFilter() {
  }
    
  /** Create a record reader for the given split
   * @param split file split
   * @param job job configuration
   * @param reporter reporter who sends report to task tracker
   * @return RecordReader
   */
  public RecordReader<K, V> getRecordReader(InputSplit split,
                                      JobConf job, Reporter reporter)
    throws IOException {
        
    reporter.setStatus(split.toString());
        
    return new FilterRecordReader<K, V>(job, (FileSplit) split);
  }


  /** set the filter class
   * 
   * @param conf application configuration
   * @param filterClass filter class
   */
  public static void setFilterClass(Configuration conf, Class filterClass) {
    conf.set(FILTER_CLASS, filterClass.getName());
  }

         
  /**
   * filter interface
   */
  public interface Filter extends 
      org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter.Filter {
  }
    
  /**
   * base class for Filters
   */
  public static abstract class FilterBase extends org.apache.hadoop.mapreduce.
      lib.input.SequenceFileInputFilter.FilterBase
      implements Filter {
  }
    
  /** Records filter by matching key to regex
   */
  public static class RegexFilter extends FilterBase {
    org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter.
      RegexFilter rf;
    public static void setPattern(Configuration conf, String regex)
        throws PatternSyntaxException {
      org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter.
        RegexFilter.setPattern(conf, regex);
    }
        
    public RegexFilter() { 
      rf = new org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter.
             RegexFilter();
    }
        
    /** configure the Filter by checking the configuration
     */
    public void setConf(Configuration conf) {
      rf.setConf(conf);
    }


    /** Filtering method
     * If key matches the regex, return true; otherwise return false
     * @see org.apache.hadoop.mapred.SequenceFileInputFilter.Filter#accept(Object)
     */
    public boolean accept(Object key) {
      return rf.accept(key);
    }
  }

  /** This class returns a percentage of records
   * The percentage is determined by a filtering frequency <i>f</i> using
   * the criteria record# % f == 0.
   * For example, if the frequency is 10, one out of 10 records is returned.
   */
  public static class PercentFilter extends FilterBase {
    org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter.
	      PercentFilter pf;
    /** set the frequency and stores it in conf
     * @param conf configuration
     * @param frequency filtering frequencey
     */
    public static void setFrequency(Configuration conf, int frequency) {
       org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter.
	      PercentFilter.setFrequency(conf, frequency);
    }
	        
    public PercentFilter() { 
      pf = new org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter.
        PercentFilter();
    }
	        
    /** configure the filter by checking the configuration
     * 
     * @param conf configuration
     */
    public void setConf(Configuration conf) {
      pf.setConf(conf);
    }

    /** Filtering method
     * If record# % frequency==0, return true; otherwise return false
     * @see org.apache.hadoop.mapred.SequenceFileInputFilter.Filter#accept(Object)
     */
    public boolean accept(Object key) {
      return pf.accept(key);
    }
  }

  /** This class returns a set of records by examing the MD5 digest of its
   * key against a filtering frequency <i>f</i>. The filtering criteria is
   * MD5(key) % f == 0.
   */
  public static class MD5Filter extends FilterBase {
    public static final int MD5_LEN = org.apache.hadoop.mapreduce.lib.
      input.SequenceFileInputFilter.MD5Filter.MD5_LEN;
    org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter.MD5Filter mf;
    /** set the filtering frequency in configuration
     * 
     * @param conf configuration
     * @param frequency filtering frequency
     */
    public static void setFrequency(Configuration conf, int frequency) {
      org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter.MD5Filter.
        setFrequency(conf, frequency);
    }
        
    public MD5Filter() { 
      mf = new org.apache.hadoop.mapreduce.lib.input.
        SequenceFileInputFilter.MD5Filter();
    }
        
    /** configure the filter according to configuration
     * 
     * @param conf configuration
     */
    public void setConf(Configuration conf) {
      mf.setConf(conf);
    }

    /** Filtering method
     * If MD5(key) % frequency==0, return true; otherwise return false
     * @see org.apache.hadoop.mapred.SequenceFileInputFilter.Filter#accept(Object)
     */
    public boolean accept(Object key) {
      return mf.accept(key);
    }
  }
    
  private static class FilterRecordReader<K, V>
    extends SequenceFileRecordReader<K, V> {
    
    private Filter filter;
        
    public FilterRecordReader(Configuration conf, FileSplit split)
      throws IOException {
      super(conf, split);
      // instantiate filter
      filter = (Filter)ReflectionUtils.newInstance(
                                                   conf.getClass(FILTER_CLASS, PercentFilter.class), 
                                                   conf);
    }
        
    public synchronized boolean next(K key, V value) throws IOException {
      while (next(key)) {
        if (filter.accept(key)) {
          getCurrentValue(value);
          return true;
        }
      }
            
      return false;
    }
  }
}
