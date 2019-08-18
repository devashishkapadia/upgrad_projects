package saavnproject;

import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;


public class Saavnpartitioner extends Partitioner<Text,DoubleWritable > implements
    Configurable {

  private Configuration configuration;
  HashMap<String, Integer> days = new HashMap<String, Integer>();
public void setConf(Configuration configuration) {
    this.configuration = configuration;
    
    days.put("25", 0);
    days.put("26", 1);
    days.put("27", 2);
    days.put("28", 3);
    days.put("29", 4);
    days.put("30", 5);
    days.put("31", 6);
    
  }

 
  public Configuration getConf() {
    return configuration;
  }

  
  public int getPartition(Text key, DoubleWritable value, int numReduceTasks)
  {String[] val = key.toString().split("\\,"); 
         String t = val[1];
	  return (int) (days.get(t.toString()));
  }
}