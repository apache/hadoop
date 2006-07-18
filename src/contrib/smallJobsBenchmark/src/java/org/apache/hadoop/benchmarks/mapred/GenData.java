package org.apache.hadoop.benchmarks.mapred;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Random;


public class GenData {
  public static final int RANDOM = 1; 
  public static final int ASCENDING = 2; 
  public static final int DESCENDING = 3; 
  
  public static void generateText(long numLines, File file, int sortType) throws IOException{
    
    PrintStream output = new PrintStream(new FileOutputStream(file)); 
    int padding = String.valueOf(numLines).length();
    
    switch(sortType){
    
    case RANDOM : 
      for(long l = 0 ; l<numLines ; l++ ){
        output.println(pad((new Random()).nextLong(), padding));
      }
      break ; 
      
    case ASCENDING: 
      for(long l = 0 ; l<numLines ; l++ ){
        output.println(pad(l, padding));
      }
      break ;
      
    case DESCENDING: 
      for(long l = numLines ; l>0 ; l-- ){
        output.println(pad(l, padding));
      }
      break ;
      
    }
    output.close() ; 
  }
  
  private static String pad( long number, int size ){
    String str = String.valueOf(number);
    
    StringBuffer value = new StringBuffer(); 
    for( int i = str.length(); i< size ; i++ ){
      value.append("0"); 
    }
    value.append(str); 
    return value.toString();
  }
  
  public static void main(String[] args){
    try{
      // test 
      generateText(100, new File("/Users/sanjaydahiya/dev/temp/sort.txt"), ASCENDING);
    }catch(Exception e){
      e.printStackTrace();
    }
  }
  
}
