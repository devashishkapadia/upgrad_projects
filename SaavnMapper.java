package saavnproject;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Date;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;

public class SaavnMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	public static List<String> dayList = Arrays.asList( "25","26","27","28","29","30","31");
 
	
 
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	 String values[] = value.toString().split("\\,");
	    String songid = values[0];
		String date = values[values.length - 1].trim();
		int Hours = Integer.parseInt(values[values.length - 2].trim());
		String dateValues[] = date.split("\\-");
		int Day = Integer.parseInt(dateValues[2]);
      
      int windowSize = 6;
    int noOfWindows = 24/windowSize;
		double weight = 0.0;
		
      
     if (Day <= 24  ){
    	  weight = (1.0 + (( Day + Hours)/noOfWindows));
    	 
    	  context.write(new Text((songid + "," + 25 ).toString()),new DoubleWritable(weight));
    }  		 
  
    
     if (Day <= 25 ){
    	 
    	 weight = (1.0 * (( Day + Hours)/noOfWindows));
    	 context.write(new Text((songid + "," + 26 ).toString()),new DoubleWritable(weight));
    }  		 
     
    if (Day <= 26 ){
    	
    	 weight = (1.0 + (( Day + Hours)/noOfWindows));
    	 context.write(new Text((songid + "," + 27).toString()),new DoubleWritable(weight));
    }  		 
     
    	 
     if (Day <= 27  ){
    	 
    	 weight = (1.0 + (( Day + Hours)/noOfWindows));
    	 context.write(new Text((songid + "," + 28).toString()),new DoubleWritable(weight));
    }  		 
     
     if (Day <= 28 ){
    	
    	 weight = (1.0 + (( Day + Hours)/noOfWindows));
    	 context.write(new Text((songid + "," + 29).toString()),new DoubleWritable(weight));
    }  		 
     
     if (Day <= 29 ){
    	
    	 weight = (1.0 + (( Day + Hours)/noOfWindows));
    	 context.write(new Text((songid + "," + 30).toString() ),new DoubleWritable(weight));
    }  		 
     
     if (Day <= 30){
    	
    	 weight = (1.0 + (( Day + Hours)/noOfWindows));
    	 context.write(new Text((songid + "," + 31).toString() ),new DoubleWritable(weight));
    }  		 
     
    }
   } 
  
  
