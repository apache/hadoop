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
package org.apache.hadoop.examples;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

public class TestWordStats {

  private final static String INPUT = "src/test/java/org/apache/hadoop/examples/pi/math";
  private final static String MEAN_OUTPUT = "build/data/mean_output";
  private final static String MEDIAN_OUTPUT = "build/data/median_output";
  private final static String STDDEV_OUTPUT = "build/data/stddev_output";

  /**
   * Modified internal test class that is designed to read all the files in the
   * input directory, and find the standard deviation between all of the word
   * lengths.
   */
  public static class WordStdDevReader {
    private long wordsRead = 0;
    private long wordLengthsRead = 0;
    private long wordLengthsReadSquared = 0;

    public WordStdDevReader() {
    }

    public double read(String path) throws IOException {
      FileSystem fs = FileSystem.get(new Configuration());
      FileStatus[] files = fs.listStatus(new Path(path));

      for (FileStatus fileStat : files) {
        if (!fileStat.isFile())
          continue;

        BufferedReader br = null;

        try {
          br = new BufferedReader(new InputStreamReader(fs.open(fileStat.getPath())));

          String line;
          while ((line = br.readLine()) != null) {
            StringTokenizer st = new StringTokenizer(line);
            String word;
            while (st.hasMoreTokens()) {
              word = st.nextToken();
              this.wordsRead++;
              this.wordLengthsRead += word.length();
              this.wordLengthsReadSquared += (long) Math.pow(word.length(), 2.0);
            }
          }

        } catch (IOException e) {
          System.out.println("Output could not be read!");
          throw e;
        } finally {
          br.close();
        }
      }

      double mean = (((double) this.wordLengthsRead) / ((double) this.wordsRead));
      mean = Math.pow(mean, 2.0);
      double term = (((double) this.wordLengthsReadSquared / ((double) this.wordsRead)));
      double stddev = Math.sqrt((term - mean));
      return stddev;
    }

  }

  /**
   * Modified internal test class that is designed to read all the files in the
   * input directory, and find the median length of all the words.
   */
  public static class WordMedianReader {
    private long wordsRead = 0;
    private TreeMap<Integer, Integer> map = new TreeMap<Integer, Integer>();

    public WordMedianReader() {
    }

    public double read(String path) throws IOException {
      FileSystem fs = FileSystem.get(new Configuration());
      FileStatus[] files = fs.listStatus(new Path(path));

      int num = 0;

      for (FileStatus fileStat : files) {
        if (!fileStat.isFile())
          continue;

        BufferedReader br = null;

        try {
          br = new BufferedReader(new InputStreamReader(fs.open(fileStat.getPath())));

          String line;
          while ((line = br.readLine()) != null) {
            StringTokenizer st = new StringTokenizer(line);
            String word;
            while (st.hasMoreTokens()) {
              word = st.nextToken();
              this.wordsRead++;
              if (this.map.get(word.length()) == null) {
                this.map.put(word.length(), 1);
              } else {
                int count = this.map.get(word.length());
                this.map.put(word.length(), count + 1);
              }
            }
          }
        } catch (IOException e) {
          System.out.println("Output could not be read!");
          throw e;
        } finally {
          br.close();
        }
      }

      int medianIndex1 = (int) Math.ceil((this.wordsRead / 2.0));
      int medianIndex2 = (int) Math.floor((this.wordsRead / 2.0));

      for (Integer key : this.map.navigableKeySet()) {
        int prevNum = num;
        num += this.map.get(key);

        if (medianIndex2 >= prevNum && medianIndex1 <= num) {
          return key;
        } else if (medianIndex2 >= prevNum && medianIndex1 < num) {
          Integer nextCurrLen = this.map.navigableKeySet().iterator().next();
          double median = (key + nextCurrLen) / 2.0;
          return median;
        }
      }
      return -1;
    }

  }

  /**
   * Modified internal test class that is designed to read all the files in the
   * input directory, and find the mean length of all the words.
   */
  public static class WordMeanReader {
    private long wordsRead = 0;
    private long wordLengthsRead = 0;

    public WordMeanReader() {
    }

    public double read(String path) throws IOException {
      FileSystem fs = FileSystem.get(new Configuration());
      FileStatus[] files = fs.listStatus(new Path(path));

      for (FileStatus fileStat : files) {
        if (!fileStat.isFile())
          continue;

        BufferedReader br = null;

        try {
          br = new BufferedReader(new InputStreamReader(fs.open(fileStat.getPath())));

          String line;
          while ((line = br.readLine()) != null) {
            StringTokenizer st = new StringTokenizer(line);
            String word;
            while (st.hasMoreTokens()) {
              word = st.nextToken();
              this.wordsRead++;
              this.wordLengthsRead += word.length();
            }
          }
        } catch (IOException e) {
          System.out.println("Output could not be read!");
          throw e;
        } finally {
          br.close();
        }
      }

      double mean = (((double) this.wordLengthsRead) / ((double) this.wordsRead));
      return mean;
    }

  }

  /**
   * Internal class designed to delete the output directory. Meant solely for
   * use before and after the test is run; this is so next iterations of the
   * test do not encounter a "file already exists" error.
   * 
   * @param dir
   *          The directory to delete.
   * @return Returns whether the deletion was successful or not.
   */
  public static boolean deleteDir(File dir) {
    if (dir.isDirectory()) {
      String[] children = dir.list();
      for (int i = 0; i < children.length; i++) {
        boolean success = deleteDir(new File(dir, children[i]));
        if (!success) {
          System.out.println("Could not delete directory after test!");
          return false;
        }
      }
    }

    // The directory is now empty so delete it
    return dir.delete();
  }

  @Before public void setup() throws Exception {
    deleteDir(new File(MEAN_OUTPUT));
    deleteDir(new File(MEDIAN_OUTPUT));
    deleteDir(new File(STDDEV_OUTPUT));
  }

  @Test public void testGetTheMean() throws Exception {
    String args[] = new String[2];
    args[0] = INPUT;
    args[1] = MEAN_OUTPUT;

    WordMean wm = new WordMean();
    ToolRunner.run(new Configuration(), wm, args);
    double mean = wm.getMean();

    // outputs MUST match
    WordMeanReader wr = new WordMeanReader();
    assertEquals(mean, wr.read(INPUT), 0.0);
  }

  @Test public void testGetTheMedian() throws Exception {
    String args[] = new String[2];
    args[0] = INPUT;
    args[1] = MEDIAN_OUTPUT;

    WordMedian wm = new WordMedian();
    ToolRunner.run(new Configuration(), wm, args);
    double median = wm.getMedian();

    // outputs MUST match
    WordMedianReader wr = new WordMedianReader();
    assertEquals(median, wr.read(INPUT), 0.0);
  }

  @Test public void testGetTheStandardDeviation() throws Exception {
    String args[] = new String[2];
    args[0] = INPUT;
    args[1] = STDDEV_OUTPUT;

    WordStandardDeviation wsd = new WordStandardDeviation();
    ToolRunner.run(new Configuration(), wsd, args);
    double stddev = wsd.getStandardDeviation();

    // outputs MUST match
    WordStdDevReader wr = new WordStdDevReader();
    assertEquals(stddev, wr.read(INPUT), 0.0);
  }

}
