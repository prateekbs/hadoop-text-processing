import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CorpusCalculator {

  public static class LineIndexMapper extends MapReduceBase
      implements Mapper<LongWritable, Text, Text, Text> {

    private final static Text word = new Text();
    private final static Text location = new Text();

    public void map(LongWritable key, Text val,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
	    	String line = val.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line);
	        int position=0;
	        while (tokenizer.hasMoreTokens()) {
	        	position=position+1;
	            word.set(tokenizer.nextToken());
	            output.collect(new Text(Integer.toString(position)),word);
	        }      
        }
  }



  public static class LineIndexReducer extends MapReduceBase
      implements Reducer<Text, Text, Text, Text> {
	  
    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      Map<String,Integer> tokenMap = new HashMap<String, Integer>();
      boolean first = true;
      StringBuilder toReturn = new StringBuilder();
      String token;
      Integer count; 
      while (values.hasNext()){
    	token=values.next().toString();
    	count=tokenMap.get(token);
    	if (count==null){
    		tokenMap.put(token,1);
    	}
    	else{
    		tokenMap.put(token,count+1);
    	}
                
      }
      Set<String> keys = tokenMap.keySet();
      for (String s : keys) {
    	  if (!first){
    		  toReturn.append(", ");
    	      first=false; 
    	  }
    	  toReturn.append(s+" ");
    	  toReturn.append(tokenMap.get(s)+", ");
      }

      output.collect(key, new Text(toReturn.toString()));
    }
  }


  /**
   * The actual main() method for our program; this is the
   * "driver" for the MapReduce job.
   */
  public static void main(String[] args) {
    JobClient client = new JobClient();
    JobConf conf = new JobConf(CorpusCalculator.class);

    conf.setJobName("CorpusCalculator");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(conf, new Path("input"));
    FileOutputFormat.setOutputPath(conf, new Path("output"));

    conf.setMapperClass(LineIndexMapper.class);
    conf.setReducerClass(LineIndexReducer.class);

    client.setConf(conf);

    try {
      JobClient.runJob(conf);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}