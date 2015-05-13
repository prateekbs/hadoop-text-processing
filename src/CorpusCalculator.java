import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CorpusCalculator {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
    private Text word = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	/**
    	 * TokenizerMapper is the first Mapper phase. We emit position in the sentence as key
    	 * the Word in that position and the entire Sentence as value.
    	 * E.g: 
    	 * sampleinput.txt has the following 2 lines:
    	 * Hello hadoop 
    	 * Welcome to hadoop
    	 * We emit the following key value pairs (key, value) in the Map Stage:
    	 * (1,<{Hello,Hello hadoop}>), (2,<{hadoop,Hello hadoop}>), 
    	 * (1,<{Welcome,Welcome to hadoop}>),(2,<{to,Welcome to hadoop}>),
    	 * (1,<{hadoop,Welcome to hadoop}>)
    	 * 
    	 * Assumptions: Input text file does not have { or } characters. Additionally, the 
    	 * punctuation has been removed in some preprocessing stage.
    	 */
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      int position=0;
      while (tokenizer.hasMoreTokens()) {
      	position=position+1;
        word.set('{'+tokenizer.nextToken()+','+line+'}');
        context.write(new Text(Integer.toString(position)),word);
      }      
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key,  Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	/** Reducer counts the number of times every word occurs at a particular position.
    	 * It obtains probability information from the count and the total number of (key, value) 
    	 * pairs. As the value we emit a value in the format (word probability, sentence). 
    	 * For the example sentences given in the mapper, we would emit (1,<Hello 0.5, hello hadoop>) and so on.
    	 */
    	Map<String,Integer> tokenMap = new HashMap<String, Integer>();
   	 Map<String,String> tokenSentenceMap = new HashMap<String, String>();
   	 boolean first = true;
        StringBuilder toReturn = new StringBuilder();
        String token,tokenSentence;
        String[] temp;
        Integer count;
        long N=0;
        double probability;
        for (Text val : values) {
       	 N+=1;
       	 tokenSentence = val.toString();
       	 temp=tokenSentence.split("\\{|\\}");
       	 token=temp[1].split(",")[0];
       	 tokenSentenceMap.put(tokenSentence,token);
       	 count=tokenMap.get(token);
       	    	if (count==null){
            		tokenMap.put(token,1);
            	}
            	else{
            		tokenMap.put(token,count+1);
            	}
            
       	 }
        assert(N>0);
        Set<String> keys = tokenSentenceMap.keySet();
        double highest_probability=0;
        for (String s : keys) {
        	if ((double)tokenMap.get(tokenSentenceMap.get(s))/N>highest_probability){
        		highest_probability=(double)tokenMap.get(tokenSentenceMap.get(s))/N;
        	}
        }
        for (String s : keys) {
      	  if (!first){
      		  toReturn.append(", ");
      	      first=false; 
      	  }
      	  toReturn.append(s+" ");
      	probability=(double)tokenMap.get(tokenSentenceMap.get(s))/(N);
      	  toReturn.append(Double.toString(probability)+",");
        }
        context.write(key, new Text(toReturn.toString()));
   }
  }
  
public static class TokenizerMapper2
  extends Mapper<Object, Text, Text, Text>{
  public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
	  /**In the second Mapper stage, we emit the sentence as the key and the probability of the word as value. 
	   * This ensures that we can multiply all the probability values in the reducer stage to get the 
	   * probability of occurrence of the sentence. 
	   * (1,<Hello 0.5, hello hadoop>)  that is received from the reducer would be emitted as 
	   * (<hello hadoop>,0.5)
	   */
   String line = value.toString();
   String[] tokenSentenceProbability=line.split("\\{|\\}|,|\\n");
   List<String> listTokenSentenceProbability = new ArrayList<String>(Arrays.asList(tokenSentenceProbability));
   listTokenSentenceProbability.removeAll(Arrays.asList("", null));
   int i=1;
   String Token,sentence,probability;
   while (i+2<listTokenSentenceProbability.size()){
		//Token=listTokenSentenceProbability.get(i);
		sentence=listTokenSentenceProbability.get(i+1);
		probability=listTokenSentenceProbability.get(i+2);
		i=i+3;
	    context.write(new Text(sentence), new Text(probability+","));
	}
 } 
}

public static class IntSumReducer2
extends Reducer<Text,Text,Text,Text> {
private Map<String, Double> countMap=new HashMap<>();
public void reduce(Text key, Iterable<Text> values,
                Context context
                ) throws IOException, InterruptedException {
	/**
	 * In the second reducer stage, we multiply all the probability values received to get the 
	 * probability of the entire sentence. We also dump all the sentences onto a Hashmap and sort by 
	 * value and get the top 3 sentences with highest probability.
	 */
	StringBuilder toReturn = new StringBuilder();
	for (Text val : values) {
		toReturn.append(val.toString());
	}
	double probability=1;
	String stringProbabilities=toReturn.toString();
	String[] probabilities=stringProbabilities.split(",");
	for (int i=0;i<probabilities.length;i++)
	{
		probability=probability*Double.parseDouble(probabilities[i]);
	}
	countMap.put(key.toString(),probability);	
}
protected void cleanup(Context context) throws IOException, InterruptedException {
	countMap=MapUtil.sortByValue(countMap);
	int counter=0;
	for(Map.Entry<String, Double> entry : countMap.entrySet()) {
		counter++;
		context.write(new Text(entry.getKey()),new Text((Double.toString(entry.getValue()))));
		if (counter==3)
			break;
	}
}

}

public static class TokenizerMapper3
extends Mapper<Object, Text, Text, Text>{
public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {
	/**
	 * The third mapper stage acts as a dummy stage that passes everything that it receives from the second reducer
	 * to the third reducer.  
	 */
String line = value.toString();
String[] sentenceProbability=line.split("\\s+");
StringBuilder toSendKey = new StringBuilder();
int position=0;
List<String> listSentenceProbability = new ArrayList<String>(Arrays.asList(sentenceProbability));
listSentenceProbability.removeAll(Arrays.asList("", null));
for (int i=0;i<listSentenceProbability.size()-1;i++){
	position++;
	toSendKey.append(listSentenceProbability.get(i)+" ");
}
toSendKey.append(",");
context.write(new Text(toSendKey.toString()),new Text(listSentenceProbability.get(position)));
}
}


public static class IntSumReducer3
extends Reducer<Text,Text,Text,Text> {
private Map<String, Double> countMap=new HashMap<>();
public void reduce(Text key, Iterable<Text> values,
                Context context
                ) throws IOException, InterruptedException {
	/**
	 * Every reducer would have got the top  3 sentences with highest probability. In this stage,
	 * we do the top 3 sentences across all reducers. We need to set the number of reducers to 1 as follows:
	 *  job3.setNumReduceTasks(1);
	 */
	StringBuilder toReturn = new StringBuilder();
	for (Text val : values) {
		toReturn.append(val.toString());
	} 
	countMap.put(key.toString(),Double.parseDouble(toReturn.toString()));	
}
protected void cleanup(Context context) throws IOException, InterruptedException {
	countMap=MapUtil.sortByValue(countMap);
	int counter=0;
	for(Map.Entry<String, Double> entry : countMap.entrySet()) {
		counter++;
		context.write(new Text(entry.getKey()),new Text((Double.toString(entry.getValue()))));
		if (counter==3)
			break;
	}
}

}


  public static void main(String[] args) throws Exception {
	/**
	 * Chaining multiple Map reduce jobs and making the later ones wait until the 
	 * previous ones have completed.   
	 */
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "CorpusCalculator");
    job.setJarByClass(CorpusCalculator.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    Path intOut=new Path("intermedoutput");
    Path intOut2=new Path("intermedoutput2");
    FileOutputFormat.setOutputPath(job,intOut);
    job.waitForCompletion(true);
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "CorpusCalculator");
    job2.setJarByClass(CorpusCalculator	.class);
    job2.setMapperClass(TokenizerMapper2.class);
    job2.setReducerClass(IntSumReducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, intOut);
    FileOutputFormat.setOutputPath(job2, intOut2);
    Configuration conf3 = new Configuration();
    job2.waitForCompletion(true);
    Job job3 = Job.getInstance(conf3, "CorpusCalculator");
    job3.setJarByClass(CorpusCalculator	.class);
    job3.setMapperClass(TokenizerMapper3.class);
    job3.setReducerClass(IntSumReducer3.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);
    job3.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job3, intOut2);
    FileOutputFormat.setOutputPath(job3, new Path(args[1]));
    System.exit(job3.waitForCompletion(true) ? 0 : 1);
  }

}