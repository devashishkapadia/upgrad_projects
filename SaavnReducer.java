package saavnproject;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Arrays;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class SaavnReducer extends Reducer<Text,DoubleWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
HashMap<String,Double> songWeight = new HashMap<String,Double>();
	double sum = 0.0;
		String[] Keyset = key.toString().split("\\,");
		String keys =  Keyset[0];
		
for (DoubleWritable val : values){
	
sum += val.get();
songWeight.put(keys, sum);



}
for (Entry<String, Double> Final : songWeight.entrySet()) {
String songId =  Final.getKey();
	Double Weight = Final.getValue();
	
	context.write(new Text(songId), new DoubleWritable(Weight));
	
}

		
	}

	

}
  
   
	

	
	
	
	
	



		
