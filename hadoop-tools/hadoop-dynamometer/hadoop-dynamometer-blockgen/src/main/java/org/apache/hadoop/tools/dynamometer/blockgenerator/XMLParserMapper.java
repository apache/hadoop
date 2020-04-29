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
package org.apache.hadoop.tools.dynamometer.blockgenerator;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This Mapper class generates a list of {@link BlockInfo}'s from a given
 * fsimage.
 *
 * Input: fsimage in XML format. It should be generated using
 * {@code org.apache.hadoop.hdfs.tools.offlineImageViewer.OfflineImageViewer}.
 *
 * Output: list of all {@link BlockInfo}'s
 */
public class XMLParserMapper
    extends Mapper<LongWritable, Text, IntWritable, BlockInfo> {

  private static final Logger LOG =
      LoggerFactory.getLogger(XMLParserMapper.class);

  @Override
  public void setup(Mapper.Context context) {
    Configuration conf = context.getConfiguration();
    numDataNodes = conf.getInt(GenerateBlockImagesDriver.NUM_DATANODES_KEY, -1);
    parser = new XMLParser();
  }

  // Blockindexes should be generated serially
  private int blockIndex = 0;
  private int numDataNodes;
  private XMLParser parser;

  /**
   * Read the input XML file line by line, and generate list of blocks. The
   * actual parsing logic is handled by {@link XMLParser}. This mapper just
   * delegates to that class and then writes the blocks to the corresponding
   * index to be processed by reducers.
   */
  @Override
  public void map(LongWritable lineNum, Text line,
      Mapper<LongWritable, Text, IntWritable, BlockInfo>.Context context)
      throws IOException, InterruptedException {
    List<BlockInfo> blockInfos = parser.parseLine(line.toString());
    for (BlockInfo blockInfo : blockInfos) {
      for (short i = 0; i < blockInfo.getReplication(); i++) {
        context.write(new IntWritable((blockIndex + i) % numDataNodes),
            blockInfo);
      }

      blockIndex++;
      if (blockIndex % 1000000 == 0) {
        LOG.info("Processed " + blockIndex + " blocks");
      }
    }
  }
}
