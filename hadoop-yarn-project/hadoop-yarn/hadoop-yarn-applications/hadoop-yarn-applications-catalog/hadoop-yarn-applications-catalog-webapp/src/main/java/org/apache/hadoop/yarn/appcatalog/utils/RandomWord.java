/*
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
package org.apache.hadoop.yarn.appcatalog.utils;

import java.util.Arrays;
import java.util.Random;

/**
 * Random word generator utility.
 */
public final class RandomWord {

  private RandomWord() {
  }

  public static String getNewWord(int wordLength) throws WordLengthException {
    if (wordLength < 3 || wordLength > 15) {
      throw new WordLengthException(
          "Word length error, words must be between 3 and 15 characters long.");
    }
    String theNewWord = "e";
    while (theNewWord.equals("e")) {
      try {
        theNewWord = generateRandomWord(wordLength);
      } catch (Exception e) {
      }
    }
    return theNewWord;
  }

  private static String generateRandomWord(int wordLength) {
    String randomWord;
    // -----------------------------------------------------------------------
    // Bigram source and general concept based on
    // https://github.com/scrollback/scrollback & described in
    // https://www.hackerearth.com/notes/random-pronouncable-text-generator/
    String[] startBiGram = {"TH", "OF", "AN", "IN", "TO", "CO", "BE", "HE",
        "RE", "HA", "WA", "FO", "WH", "MA", "WI", "ON", "HI", "PR", "ST", "NO",
        "IS", "IT", "SE", "WE", "AS", "CA", "DE", "SO", "MO", "SH", "DI", "AL",
        "AR", "LI", "WO", "FR", "PA", "ME", "AT", "SU", "BU", "SA", "FI", "NE",
        "CH", "PO", "HO", "DO", "OR", "UN", "LO", "EX", "BY", "FA", "LA", "LE",
        "PE", "MI", "SI", "YO", "TR", "BA", "GO", "BO", "GR", "TE", "EN", "OU",
        "RA", "AC", "FE", "PL", "CL", "SP", "BR", "EV", "TA", "DA", "AB", "TI",
        "RO", "MU", "EA", "NA", "SC", "AD", "GE", "YE", "AF", "AG", "UP", "AP",
        "DR", "US", "PU", "CE", "IF", "RI", "VI", "IM", "AM", "KN", "OP", "CR",
        "OT", "JU", "QU", "TW", "GA", "VA", "VE", "PI", "GI", "BI", "FL", "BL",
        "EL", "JO", "FU", "HU", "CU", "RU", "OV", "MY", "OB", "KE", "EF", "PH",
        "CI", "KI", "NI", "SL", "EM", "SM", "VO", "MR", "WR", "ES", "DU", "TU",
        "AU", "NU", "GU", "OW", "SY", "JA", "OC", "EC", "ED", "ID", "JE", "AI",
        "EI", "SK", "OL", "GL", "EQ", "LU", "AV", "SW", "AW", "EY", "TY"};
    String[] lookupBiGram = {"TH", "AN", "IN", "IO", "EN", "TI", "FO", "HE",
        "HA", "HI", "TE", "AT", "ER", "AL", "WA", "VE", "CO", "RE", "IT", "WI",
        "ME", "NC", "ON", "PR", "AR", "ES", "EV", "ST", "EA", "IV", "EC", "NO",
        "OU", "PE", "IL", "IS", "MA", "AV", "OM", "IC", "GH", "DE", "AI", "CT",
        "IG", "ID", " OR", "OV", "UL", "YO", "BU", "RA", "FR", "RO", "WH", "OT",
        "BL", "NT", "UN", "TR", "HO", "AC", "TU", "WE", "CA", "SH", "UR", "IE",
        "PA", "TO", "EE", "LI", "RI", "UG", "AM", "ND", "US", "LL", "AS", "TA",
        "LE", "MO", "WO", "MI", "AB", "EL", "IA", "NA", "SS", "AG", "TT", "NE",
        "PL", " LA", "OS", "CE", "DI", "BE", "AP", "SI", "NI", "OW", "SO", "AK",
        "CH", "EM", "IM", "SE", "NS", "PO", "EI", "EX", "KI", "UC", "AD", "GR",
        "IR", "NG", "OP", "SP", "OL", "DA", "NL", "TL", "LO", "BO", "RS", "FE",
        "FI", "SU", "GE", "MP", "UA", "OO", "RT", "SA", "CR", "FF", "IK", "MB",
        " KE", "FA", "CI", "EQ", "AF", "ET", "AY", "MU", "UE", "HR", "TW", "GI",
        "OI", "VI", "CU", "FU", "ED", "QU", "UT", "RC", "OF", "CL", "FT", "IZ",
        "PP", "RG", "DU", "RM", "YE", "RL", "DO", "AU", "EP", "BA", "JU", "RD",
        "RU", "OG", "BR", "EF", "KN", "LS", "GA", "PI", "YI", "BI", "IB", "UB",
        "VA", "OC", "IF", "RN", "RR", "SC", "TC", "CK", "DG", "DR", "MM", "NN",
        "OD", "RV", "UD", "XP", "JE", "UM", "EG", "DL", "PH", "SL", "GO", "CC",
        "LU", "OA", "PU", "UI", "YS", "ZA", "HU", "MR", "OE", "SY", "EO", "TY",
        "UP", "FL", "LM", "NF", "RP", "OH", "NU", "XA", "OB", "VO", "DM", "GN",
        " LD", "PT", "SK", "WR", "JO", "LT", "YT", "UF", "BJ", "DD", "EY", "GG",
        "GL", "GU", "HT", "LV", "MS", "NM", "NV", "OK", "PM", "RK", "SW", "TM",
        "XC", "ZE", "AW", "SM"};
    String[][][] nextCharLookup = {
        {{"E", "A", "I", "O", "R"}, {"E", "O"}},
        {{"D", "T", "Y", "C", "S", "G", "N", "I", "O", "E", "A", "K"},
            {"D", "T", "Y", "S", "G", "O", "E", "A", "K"}},
        {{"G", "T", "E", "D", "S", "C", "A", "I", "K", "V", "U", "N", "F"},
            {"G", "T", "E", "D", "S", "A", "K"}},
        {{"N", "U", "R"}, {"N", "U", "R"}},
        {{"T", "C", "D", "S", "E", "I", "G", "O", "N", "A"},
            {"T", "D", "S", "E", "G", "O", "A"}},
        {{"O", "N", "C", "V", "M", "L", "E", "T", "S", "A", "R", "F"},
            {"N", "C", "M", "L", "E", "T", "S", "A", "R", "F"}},
        {{"R", "U", "O", "L"}, {"R", "U", "O", "L"}},
        {{"R", "N", "Y", "S", "M", "I", "A", "L", "D", "T"},
            {"R", "N", "Y", "S", "M", "A", "L", "D", "T"}},
        {{"T", "D", "V", "N", "S", "R", "P", "L"},
            {"T", "D", "N", "S", "R", "L"}},
        {{"S", "N", "C", "M", "L", "P", "G", "T", "R", "E"},
            {"S", "N", "C", "M", "L", "P", "G", "T", "R", "E"}},
        {{"R", "D", "N", "S", "M", "L", "E", "C", "A"},
            {"R", "D", "N", "S", "M", "L", "E", "A"}},
        {{"I", "E", "T", "H", "U", "O", "C"}, {"E", "H", "O"}},
        {{"E", "S", "I", "A", "N", "Y", "T", "V", "M", "R", "O", "L", "G",
             "F", "C"}, {"E", "S", "A", "N", "Y", "T", "M"}},
        {{"L", "S", "I", "T", "E", "U", "O", "M", "K", "F", "A"},
            {"L", "S", "T", "E", "F"}},
        {{"S", "Y", "R", "T", "N", "L"}, {"S", "Y", "R", "T", "N", "L"}},
        {{"R", "N", "L", "S", "D"}, {"R", "N", "L", "S", "D"}},
        {{"N", "M", "U", "R", "L", "V", "S", "O"},
            {"N", "M", "U", "R", "L", "O"}},
        {{"S", "A", "D", "N", "E", "C", "L", "T", "P", "M", "V", "G", "F",
             "Q"}, {"S", "A", "D", "N", "E", "L", "T", "P", "M"}},
        {{"H", "I", "Y", "E", "S", "T", "A", "U"},
            {"H", "Y", "E", "S", "A"}},
        {{"T", "L", "N", "S"}, {"T", "L", "N", "S"}},
        {{"N", "R", "D", "T", "S", "M", "A"},
            {"N", "R", "D", "T", "S", "M", "A"}},
        {{"E", "I", "H", "T", "R", "O", "L"}, {"E", "H", "T"}},
        {{"S", "E", "T", "G", "A", "D", "L", "C", "V", "O", "I", "F"},
            {"S", "E", "T", "G", "A", "D", "O"}},
        {{"O", "E", "I", "A"}, {"E", "A"}},
        {{"E", "T", "D", "Y", "S", "I", "R", "L", "M", "K", "G", "A", "O",
             "N", "C"}, {"E", "T", "D", "Y", "S", "M", "K", "A", "N"}},
        {{"S", "T", "E", "I", "P", "U", "C"}, {"S", "T", "E"}},
        {{"E", "I"}, {"E"}},
        {{"A", "R", "I", "E", "O", "U", "S"}, {"A", "E", "O", "S"}},
        {{"R", "S", "T", "D", "L", "C", "N", "V", "M", "K"},
            {"R", "S", "T", "D", "L", "N", "M"}},
        {{"E", "I", "A"}, {"E"}},
        {{"T", "O", "I", "E", "A", "U", "R", "H"}, {"T", "E", "H"}},
        {{"T", "W", "R", "U", "N", "M"}, {"T", "W", "R", "U", "N", "M"}},
        {{"T", "L", "R", "N", "S", "G", "P", "B"},
            {"T", "L", "R", "N", "S", "P"}},
        {{"R", "N", "C", "A", "D", "T", "O"}, {"R", "N", "A", "D", "T"}},
        {{"L", "E", "I", "Y", "D", "A"}, {"L", "E", "Y", "D"}},
        {{"T", "H", "S", "I", "E", "C", "M"}, {"T", "H", "S", "E", "M"}},
        {{"N", "T", "L", "K", "D", "S", "I", "G"},
            {"N", "T", "L", "D", "S"}},
        {{"E", "I", "A"}, {"E"}},
        {{"E", "P", "M", "I", "A"}, {"E"}},
        {{"A", "H", "E", "I", "T", "K", "U", "S"},
            {"H", "E", "T", "K", "S"}},
        {{"T"}, {"T"}},
        {{"R", "N", "S", "D", "A", "V", "P", "T", "M", "L", "F"},
            {"R", "N", "S", "D", "A", "P", "T", "M", "L"}},
        {{"N", "D", "R", "L", "T"}, {"N", "D", "R", "L", "T"}},
        {{"I", "E", "U", "S", "O"}, {"E", "S", "O"}},
        {{"H", "N", "I"}, {"H", "N"}}, {{"E"}, {"E"}},
        {{"E", "T", "M", "D", "S", "K", "I", "Y", "L", "G", "A", "R", "N",
             "C"}, {"E", "T", "M", "D", "S", "K", "Y", "A", "N"}},
        {{"E", "I"}, {"E"}},
        {{"D", "T", "A", "L"}, {"D", "T", "L"}}, {{"U"}, {"U"}},
        {{"T", "S", "R", "I"}, {"T", "S", "R"}},
        {{"T", "N", "L", "C", "I", "M", "D", "S", "R", "P", "G", "B"},
            {"T", "N", "L", "M", "D", "S", "R"}},
        {{"O", "E", "A"}, {"E", "A"}},
        {{"M", "U", "V", "P", "N", "W", "S", "O", "L", "D", "C", "B", "A",
             "T", "G"}, {"M", "U", "P", "N", "W", "O", "L", "D", "T"}},
        {{"I", "E", "O", "A"}, {"E", "O"}},
        {{"H", "E", "T", "I"}, {"H", "E"}},
        {{"E", "I", "Y", "O", "A"}, {"E", "Y"}},
        {{"E", "I", "S", "R", "O", "A", "L", "Y", "U", "H"},
            {"E", "S", "O", "A", "Y", "H"}},
        {{"D", "T", "I", "C", "G"}, {"D", "T", "G"}},
        {{"A", "I", "O", "E", "U", "Y"}, {"A", "E", "Y"}},
        {{"U", "W", "S", "R", "L", "O", "M", "T", "P", "N", "D"},
            {"U", "W", "R", "L", "O", "M", "T", "P", "N", "D"}},
        {{"T", "E", "K", "H", "C", "R", "I"}, {"T", "E", "K", "H"}},
        {{"R", "D", "A", "T"}, {"R", "T"}},
        {{"R", "L", "E", "V", "S", "N", "A"},
            {"R", "L", "E", "S", "N", "A"}},
        {{"L", "N", "T", "R", "U", "S", "M", "P"},
            {"L", "N", "T", "R", "S", "M"}},
        {{"E", "O", "I", "A"}, {"E", "O"}},
        {{"E", "N", "T", "S", "I", "A", "Y", "R", "P", "C"},
            {"E", "N", "T", "S", "A", "Y"}},
        {{"S", "N", "D", "T", "W", "V", "R", "L", "F"},
            {"S", "N", "D", "T", "W", "R", "L"}},
        {{"R", "T", "S", "N", "L", "I", "C"}, {"R", "T", "S", "N", "L"}},
        {{"R", "O", "N", "W", "P", "M", "L"},
            {"R", "O", "N", "W", "P", "M", "L"}},
        {{"N", "D", "T", "M", "S", "R", "P", "L", "K"},
            {"N", "D", "T", "M", "S", "R", "P", "L", "K"}},
        {{"N", "T", "S", "C", "K", "G", "E", "F", "Z", "V", "O", "M", "A"},
            {"N", "T", "S", "C", "G", "E", "F", "M", "A"}},
        {{"N", "E", "C", "T", "S", "G", "A", "V", "O", "P", "M", "L", "D",
             "B"}, {"N", "E", "C", "T", "S", "G", "A", "P", "M", "L", "D"}},
        {{"H", "G"}, {"H"}}, {{"E", "P", "I", "O", "A"}, {"E"}},
        {{"E", "I", "S", "A", "U", "O"}, {"E", "S", "O"}},
        {{"E", "T", "I", "S", "L", "H"}, {"E", "T", "S", "H"}},
        {{"Y", "E", "O", "I", "S", "A"}, {"Y", "E", "S"}},
        {{"T", "S", "E", "I", "U", "O", "K", "H"},
            {"T", "S", "E", "O", "H"}},
        {{"T", "N", "L", "I", "R", "K", "B", "G", "C"},
            {"T", "N", "L", "R"}},
        {{"S", "D", "A", "T", "C", "R", "N", "M", "G", "V", "F"},
            {"S", "D", "A", "T", "R", "N", "M"}},
        {{"R", "S", "V", "T", "U", "D"}, {"R", "T", "U", "D"}},
        {{"R", "U"}, {"R", "U"}},
        {{"N", "L", "S", "T", "C", "G"}, {"N", "L", "S", "T", "C", "G"}},
        {{"L", "O", "I"}, {}},
        {{"L", "Y", "I", "E", "F", "O", "A", "T", "S", "P", "D"},
            {"L", "Y", "E", "F", "T", "S", "D"}},
        {{"L", "N", "T"}, {"L", "N", "T"}},
        {{"L", "T", "R", "N", "M"}, {"L", "T", "R", "N", "M"}},
        {{"I", "E", "U", "O", "A"}, {"E", "O"}},
        {{"E", "A", "O"}, {"E", "O"}}, {{"E", "L", "I"}, {"E"}},
        {{"D", "S", "W", "R", "E", "Y", "V", "T", "L", "C", "A"},
            {"D", "S", "W", "R", "E", "Y", "T", "L", "A"}},
        {{"A", "E", "I", "Y", "O"}, {"E", "Y"}},
        {{"T", "N", "R", "S", "C", "Y", "W", "I", "B"},
            {"T", "N", "R", "S", "Y", "W"}},
        {{"T", "E", "S", "I"}, {"T", "E", "S"}},
        {{"S", "N", "R", "D", "P", "L", "I"},
            {"S", "N", "R", "D", "P", "L"}},
        {{"S", "N", "T", "D", "F", "E", "C", "A", "V", "R"},
            {"S", "N", "T", "D", "F", "E", "C", "A", "R"}},
        {{"R", "E", "C", "T", "L", "F", "S", "I", "G", "D", "A"},
            {"R", "E", "T", "L", "S", "D", "A"}},
        {{"P", "E", "A"}, {"E"}},
        {{"O", "N", "D", "T", "S", "G", "C", "B", "V", "M", "A"},
            {"N", "D", "T", "S", "G", "C", "M", "A"}},
        {{"N", "T", "S", "C", "Z", "O", "G", "F"},
            {"N", "T", "S", "C", "G", "F"}},
        {{"N", "E", "S", "I", "A"}, {"N", "E", "S"}},
        {{"N", "M", "U", "L", "C", "R"}, {"N", "M", "U", "L", "R"}},
        {{"E", "I"}, {"E"}},
        {{"E", "A", "I", "O", "U", "R"}, {"E", "O"}},
        {{"E", "S", "P", "O", "B", "A", "I"}, {"E", "S"}},
        {{"E", "P", "I", "A", "S", "M"}, {"E", "S"}},
        {{"D", "N", "L", "S", "R", "E", "C", "T", "V", "A"},
            {"D", "N", "L", "S", "R", "E", "T", "A"}},
        {{"T", "I", "E"}, {"T", "E"}},
        {{"S", "R", "N", "L", "W", "T", "I"}, {"R", "N", "L", "W", "T"}},
        {{"R", "N", "G", "T"}, {"R", "N", "G", "T"}},
        {{"P", "T", "I", "C", "A"}, {"T"}}, {{"N"}, {"N"}},
        {{"H", "T", "K", "E"}, {"H", "T", "K", "E"}},
        {{"E", "I", "Y", "V", "M", "D"}, {"E", "Y"}},
        {{"E", "A", "O"}, {"E", "A"}},
        {{"E", "S", "T", "L", "I"}, {"E", "S", "T"}},
        {{"E", "S", "L", "T", "R", "I"}, {"E", "S"}},
        {{"E", "P", "L"}, {"E"}}, {{"E", "O", "I", "A"}, {"E"}},
        {{"D", "L", "I", "O", "E", "U"}, {"D", "L", "E"}},
        {{"Y", "T", "R", "N"}, {"Y", "T", "R", "N"}},
        {{"Y"}, {"Y"}}, {{"Y", "E"}, {"Y", "E"}},
        {{"W", "N", "O", "S", "C", "V", "U", "T", "R", "P", "G"},
            {"W", "N", "O", "U", "T", "R", "P"}},
        {{"U", "T", "R", "O", "D", "A"}, {"U", "T", "R", "O", "D"}},
        {{"T", "E", "O", "I"}, {"T", "E", "O"}},
        {{"R", "E", "W", "L", "C", "A"}, {"R", "E", "W", "L", "A"}},
        {{"R", "N", "C", "E", "L", "G"}, {"R", "N", "C", "E", "L", "G"}},
        {{"R", "C", "P", "B", "M", "L", "A"}, {"R", "P", "M", "L"}},
        {{"N", "T", "S", "R", "D"}, {"N", "T", "S", "R", "D"}},
        {{"L", "O", "A", "T", "R", "E"}, {"T", "E"}},
        {{"L", "T", "R"}, {"L", "T", "R"}},
        {{"K", "D", "L", "T", "R", "N", "M"},
            {"K", "D", "L", "T", "R", "N", "M"}},
        {{"I", "H", "A", "E", "Y", "U", "S"}, {"H", "A", "E", "Y", "S"}},
        {{"I", "M", "Y", "N", "L"}, {"M", "Y", "N", "L"}},
        {{"E", "I", "O", "A"}, {"E", "A"}}, {{"E", "I"}, {"E"}},
        {{"E"}, {"E"}}, {{"E"}, {"E"}},
        {{"D", "N", "T", "S", "R", "E"}, {"D", "N", "T", "S", "R", "E"}},
        {{"C", "R", "M", "I"}, {"R", "M"}},
        {{"A", "T", "E", "S", "P", "N"}, {"A", "T", "E", "S", "P", "N"}},
        {{"U"}, {}}, {{"T", "F"}, {"T", "F"}},
        {{"T", "I", "H", "E", "Y", "W", "S", "A"},
            {"H", "E", "Y", "S", "A"}},
        {{"S", "E"}, {"S"}},
        {{"S", "N", "L", "C"}, {"S", "N", "L"}},
        {{"S", "N"}, {"S", "N"}}, {{"O", "E", "I"}, {"E"}},
        {{"O", "E"}, {"O", "E"}},
        {{"N", "V", "O", "C"}, {"N", "C"}}, {{"N"}, {"N"}},
        {{"N", "D", "S", "C", "T", "O", "L", "E"},
            {"N", "D", "S", "C", "T", "L", "E"}},
        {{"L", "R", "T", "S"}, {"L", "R", "T", "S"}},
        {{"L", "R", "N"}, {"L", "R", "N"}},
        {{"I", "U", "E"}, {"E"}}, {{"I", "E", "A"}, {"E"}},
        {{"I", "H", "E"}, {"H", "E"}}, {{"H", "E"}, {"H", "E"}},
        {{"F", "T"}, {"F", "T"}}, {{"E", "A", "U", "O"}, {"E"}},
        {{"E"}, {"E"}}, {{"E", "A"}, {"E"}},
        {{"E", "O", "R", "L"}, {"E"}}, {{"E", "A"}, {"E"}},
        {{"C", "S", "R", "A"}, {"S", "R"}},
        {{"A", "S", "I", "E"}, {"S", "E"}},
        {{"A", "S", "D"}, {"A", "S", "D"}},
        {{"Y", "D"}, {"Y", "D"}},
        {{"W", "N", "M", "E"}, {"W", "N", "M"}},
        {{"T", "S"}, {"T", "S"}},
        {{"T", "O", "E", "A"}, {"T", "E"}},
        {{"S", "C", "R", "N", "L"}, {"S", "R", "N", "L"}},
        {{"S"}, {"S"}}, {{"S", "E", "I"}, {"S", "E"}},
        {{"S", "N", "C"}, {"S", "N"}}, {{"R", "I"}, {}},
        {{"O", "I", "E", "A"}, {"E", "A"}},
        {{"O", "F", "U", "T", "E"}, {"F", "T", "E"}},
        {{"O", "E"}, {"O", "E"}}, {{"O"}, {"O"}},
        {{"N", "I", "T", "R"}, {"N", "T", "R"}},
        {{"N", "T", "R", "E", "C"}, {"N", "T", "R", "E", "C"}},
        {{"N"}, {"N"}}, {{"L", "T", "N"}, {"L", "T", "N"}},
        {{"L", "I", "E"}, {"E"}}, {{"L"}, {}},
        {{"L", "T", "R", "N"}, {"L", "T", "R", "N"}},
        {{"K", "I", "E", "C", "A"}, {"K", "E"}},
        {{"I", "F", "E", "T"}, {"F", "E", "T"}},
        {{"I", "E", "M", "A"}, {"E", "A"}},
        {{"I", "E", "Y", "O"}, {"E", "Y"}},
        {{"H", "R", "O", "I", "A"}, {"H"}}, {{"H"}, {"H"}},
        {{"E"}, {"E"}}, {{"E"}, {"E"}},
        {{"E", "O", "I", "A"}, {"E", "A"}},
        {{"E", "U", "I"}, {"E"}}, {{"E", "O", "I"}, {"E", "O"}},
        {{"E", "Y", "U"}, {"E", "Y"}}, {{"E", "I"}, {"E"}},
        {{"E", "I"}, {"E"}}, {{"E"}, {"E"}}, {{"C"}, {}},
        {{"B", "E"}, {"E"}}, {{"A", "R", "I", "E"}, {"E"}},
        {{"Y", "E"}, {"Y", "E"}},
        {{"Y", "O", "I", "E"}, {"Y", "O", "E"}},
        {{"Y", "A"}, {"Y"}}, {{"V", "T", "O"}, {"T", "O"}},
        {{"U", "O", "E"}, {"E"}},
        {{"T", "S", "M", "E", "D"}, {"T", "S", "M", "E"}},
        {{"T", "R", "D"}, {"T", "R", "D"}},
        {{"T", "R", "L", "B"}, {"T", "R", "L"}},
        {{"T", "R", "L"}, {"T", "R", "L"}}, {{"T"}, {"T"}},
        {{"T"}, {"T"}},
        {{"S", "R", "N", "M"}, {"S", "R", "N", "M"}},
        {{"S"}, {"S"}}, {{"S"}, {"S"}}, {{"S"}, {"S"}},
        {{"R", "P"}, {"R", "P"}}, {{"P"}, {}}, {{"P", "O"}, {}},
        {{"O", "E"}, {"E"}}, {{"O"}, {}}, {{"O"}, {}},
        {{"O"}, {}}, {{"N"}, {}}, {{"M"}, {"M"}},
        {{"M"}, {"M"}}, {{"L"}, {}}, {{"L"}, {"L"}},
        {{"I"}, {}}, {{"I"}, {}}, {{"I", "E"}, {"E"}},
        {{"I"}, {}}, {{"I", "E"}, {"E"}}, {{"I"}, {}},
        {{"H"}, {}}, {{"H", "E"}, {"H", "E"}}, {{"H"}, {"H"}},
        {{"F"}, {"F"}}, {{"E"}, {}}, {{"E"}, {"E"}},
        {{"E"}, {}}, {{"E"}, {"E"}}, {{"E", "A"}, {"E"}},
        {{"E"}, {"E"}}, {{"E"}, {"E"}}, {{"E"}, {"E"}},
        {{"E"}, {"E"}}, {{"E"}, {"E"}}, {{"E"}, {"E"}},
        {{"E"}, {"E"}}, {{"E"}, {"E"}}, {{"E"}, {"E"}},
        {{"E"}, {"E"}}, {{"E"}, {"E"}}, {{"E"}, {"E"}},
        {{"D"}, {"D"}}, {{"A"}, {}}, {{"A"}, {}}};
    // ------------------------------------------------------------------------
    randomWord = startBiGram[indexGenerator(startBiGram.length)];
    int flag = 0;
    int count = 0;
    String previousWord;
    while (randomWord.length() != wordLength) {
      previousWord = randomWord;
      randomWord = addCharacter(startBiGram, wordLength, randomWord,
          lookupBiGram, nextCharLookup, flag);
      if (previousWord.equals(randomWord)) {
        count++;
      } else {
        flag = 0;
      }
      if (count == 5) {
        flag = 1;
        count++;
      } else if (count == 20) {
        randomWord = startBiGram[indexGenerator(startBiGram.length)];
        count = 0;
      }
    }
    return randomWord;
  }

  private static String addCharacter(String[] startBiGram, int desiredLength,
      String currentWord, String[] lookupBiGram, String[][][] nextCharLookup,
      int flag) {
    int mainIndex = getLookupIndex(currentWord, lookupBiGram);
    int type = 0;
    if (currentWord.length() == (desiredLength - 1)) {
      type = 1;
    }
    while (mainIndex < 0 || mainIndex > 263
        || nextCharLookup[mainIndex][type].length <= 0) {
      if (currentWord.length() == 2) {
        return startBiGram[indexGenerator(startBiGram.length)];
      }
      if (flag == 1) {
        currentWord = backtrack(currentWord, 2);
        flag = 0;
      } else {
        currentWord = backtrack(currentWord, 1);
      }
      mainIndex = getLookupIndex(currentWord, lookupBiGram);
      if (type == 1) {
        type = 0;
      }
    }
    String updatedWord = currentWord
        + getNextCharacter(type, mainIndex, nextCharLookup);
    return updatedWord;
  }

  private static int indexGenerator(int arrayLength) {
    int theIndex;
    Random generator = new Random();
    theIndex = generator.nextInt(arrayLength);
    return theIndex;
  }

  private static String getNextCharacter(int type, int mainIndex,
      String[][][] theCharacterVault) {
    String nextChar;
    int i = indexGenerator(theCharacterVault[mainIndex][type].length);
    nextChar = theCharacterVault[mainIndex][type][i];
    return nextChar;
  }

  private static String backtrack(String theWord, int numberChars) {
    theWord = theWord.substring(0, theWord.length() - numberChars);
    return theWord;
  }

  private static int getLookupIndex(String theWord, String[] lookupArray) {
    String lookupCharacters = theWord.substring(theWord.length() - 2);
    int lookupIndex = Arrays.asList(lookupArray).indexOf(lookupCharacters);
    return lookupIndex;
  }
}
