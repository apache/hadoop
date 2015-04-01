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
package org.apache.hadoop.examples.terasort;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.math.BigInteger;
import java.util.zip.Checksum;

import org.apache.hadoop.util.PureJavaCrc32;

/** 
 * A single process data generator for the terasort data. Based on gensort.c 
 * version 1.1 (3 Mar 2009) from Chris Nyberg &lt;chris.nyberg@ordinal.com&gt;.
 */
public class GenSort {

  /**
   * Generate a "binary" record suitable for all sort benchmarks *except* 
   * PennySort.
   */
  static void generateRecord(byte[] recBuf, Unsigned16 rand, 
                                     Unsigned16 recordNumber) {
    /* generate the 10-byte key using the high 10 bytes of the 128-bit
     * random number
     */
    for(int i=0; i < 10; ++i) {
      recBuf[i] = rand.getByte(i);
    }

    /* add 2 bytes of "break" */
    recBuf[10] = 0x00;
    recBuf[11] = 0x11;

    /* convert the 128-bit record number to 32 bits of ascii hexadecimal
     * as the next 32 bytes of the record.
     */
    for (int i = 0; i < 32; i++) {
      recBuf[12 + i] = (byte) recordNumber.getHexDigit(i);
    }

    /* add 4 bytes of "break" data */
    recBuf[44] = (byte) 0x88;
    recBuf[45] = (byte) 0x99;
    recBuf[46] = (byte) 0xAA;
    recBuf[47] = (byte) 0xBB;

    /* add 48 bytes of filler based on low 48 bits of random number */
    for(int i=0; i < 12; ++i) {
      recBuf[48+i*4] = recBuf[49+i*4] = recBuf[50+i*4] = recBuf[51+i*4] =
        (byte) rand.getHexDigit(20 + i);
    }

    /* add 4 bytes of "break" data */
    recBuf[96] = (byte) 0xCC;
    recBuf[97] = (byte) 0xDD;
    recBuf[98] = (byte) 0xEE;
    recBuf[99] = (byte) 0xFF;
  }


  private static BigInteger makeBigInteger(long x) {
    byte[] data = new byte[8];
    for(int i=0; i < 8; ++i) {
      data[i] = (byte) (x >>> (56 - 8*i));
    }
    return new BigInteger(1, data);
  }

  private static final BigInteger NINETY_FIVE = new BigInteger("95");

  /**
   * Generate an ascii record suitable for all sort benchmarks including 
   * PennySort.
   */
  static void generateAsciiRecord(byte[] recBuf, Unsigned16 rand, 
                                  Unsigned16 recordNumber) {

    /* generate the 10-byte ascii key using mostly the high 64 bits.
     */
    long temp = rand.getHigh8();
    if (temp < 0) {
      // use biginteger to avoid the negative sign problem
      BigInteger bigTemp = makeBigInteger(temp);
      recBuf[0] = (byte) (' ' + (bigTemp.mod(NINETY_FIVE).longValue()));
      temp = bigTemp.divide(NINETY_FIVE).longValue();
    } else {
      recBuf[0] = (byte) (' ' + (temp % 95));
      temp /= 95;      
    }
    for(int i=1; i < 8; ++i) {
      recBuf[i] = (byte) (' ' + (temp % 95));
      temp /= 95;      
    }
    temp = rand.getLow8();
    if (temp < 0) {
      BigInteger bigTemp = makeBigInteger(temp);
      recBuf[8] = (byte) (' ' + (bigTemp.mod(NINETY_FIVE).longValue()));
      temp = bigTemp.divide(NINETY_FIVE).longValue();      
    } else {
      recBuf[8] = (byte) (' ' + (temp % 95));
      temp /= 95;
    }
    recBuf[9] = (byte)(' ' + (temp % 95));

    /* add 2 bytes of "break" */
    recBuf[10] = ' ';
    recBuf[11] = ' ';

    /* convert the 128-bit record number to 32 bits of ascii hexadecimal
     * as the next 32 bytes of the record.
     */
    for (int i = 0; i < 32; i++) {
      recBuf[12 + i] = (byte) recordNumber.getHexDigit(i);
    }

    /* add 2 bytes of "break" data */
    recBuf[44] = ' ';
    recBuf[45] = ' ';

    /* add 52 bytes of filler based on low 48 bits of random number */
    for(int i=0; i < 13; ++i) {
      recBuf[46+i*4] = recBuf[47+i*4] = recBuf[48+i*4] = recBuf[49+i*4] =
        (byte) rand.getHexDigit(19 + i);
    }

    /* add 2 bytes of "break" data */
    recBuf[98] = '\r';	/* nice for Windows */
    recBuf[99] = '\n';
}


  private static void usage() {
    PrintStream out = System.out;
    out.println("usage: gensort [-a] [-c] [-bSTARTING_REC_NUM] NUM_RECS FILE_NAME");
    out.println("-a        Generate ascii records required for PennySort or JouleSort.");
    out.println("          These records are also an alternative input for the other");
    out.println("          sort benchmarks.  Without this flag, binary records will be");
    out.println("          generated that contain the highest density of randomness in");
    out.println("          the 10-byte key.");
    out.println( "-c        Calculate the sum of the crc32 checksums of each of the");
    out.println("          generated records and send it to standard error.");
    out.println("-bN       Set the beginning record generated to N. By default the");
    out.println("          first record generated is record 0.");
    out.println("NUM_RECS  The number of sequential records to generate.");
    out.println("FILE_NAME The name of the file to write the records to.\n");
    out.println("Example 1 - to generate 1000000 ascii records starting at record 0 to");
    out.println("the file named \"pennyinput\":");
    out.println("    gensort -a 1000000 pennyinput\n");
    out.println("Example 2 - to generate 1000 binary records beginning with record 2000");
    out.println("to the file named \"partition2\":");
    out.println("    gensort -b2000 1000 partition2");
    System.exit(1);
  }


  public static void outputRecords(OutputStream out,
                                   boolean useAscii,
                                   Unsigned16 firstRecordNumber,
                                   Unsigned16 recordsToGenerate,
                                   Unsigned16 checksum
                                   ) throws IOException {
    byte[] row = new byte[100];
    Unsigned16 recordNumber = new Unsigned16(firstRecordNumber);
    Unsigned16 lastRecordNumber = new Unsigned16(firstRecordNumber);
    Checksum crc = new PureJavaCrc32();
    Unsigned16 tmp = new Unsigned16();
    lastRecordNumber.add(recordsToGenerate);
    Unsigned16 ONE = new Unsigned16(1);
    Unsigned16 rand = Random16.skipAhead(firstRecordNumber);
    while (!recordNumber.equals(lastRecordNumber)) {
      Random16.nextRand(rand);
      if (useAscii) {
        generateAsciiRecord(row, rand, recordNumber);
      } else {
        generateRecord(row, rand, recordNumber);
      }
      if (checksum != null) {
        crc.reset();
        crc.update(row, 0, row.length);
        tmp.set(crc.getValue());
        checksum.add(tmp);
      }
      recordNumber.add(ONE);
      out.write(row);
    }
  }
                                   
  public static void main(String[] args) throws Exception {
    Unsigned16 startingRecord = new Unsigned16();
    Unsigned16 numberOfRecords;
    OutputStream out;
    boolean useAscii = false;
    Unsigned16 checksum = null;

    int i;
    for(i=0; i < args.length; ++i) {
      String arg = args[i];
      int argLength = arg.length();
      if (argLength >= 1 && arg.charAt(0) == '-') {
        if (argLength < 2) {
          usage();
        }
        switch (arg.charAt(1)) {
        case 'a':
          useAscii = true;
          break;
        case 'b':
          startingRecord = Unsigned16.fromDecimal(arg.substring(2));
          break;
        case 'c':
          checksum = new Unsigned16();
          break;
        default:
          usage();
        }
      } else {
        break;
      }
    }
    if (args.length - i != 2) {
      usage();
    }
    numberOfRecords = Unsigned16.fromDecimal(args[i]);
    out = new FileOutputStream(args[i+1]);

    outputRecords(out, useAscii, startingRecord, numberOfRecords, checksum);
    out.close();
    if (checksum != null) {
      System.out.println(checksum);
    }
  }

}
