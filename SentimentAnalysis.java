import com.sun.tools.internal.ws.wsdl.document.jaxws.Exception;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SentimentAnalysis {

    public static class SentimentSplit extends Mapper<Object, Text, Text, IntWritable> {

        public Map<String, String> Dict = new HashMap<String, String>();    //create a dictionary of word and sentiment. The key-value pair is word-sentiment

        @Override
        public void setup(Context context) throws IOException{//create dictionary of word-sentiment at the beginning

            Configuration conf;
            conf = context.getConfiguration(); //provide access to configuration parameters

            String dicName = conf.get("dictionary");

            BufferedReader br = new BufferedReader(new FileReader(dicName));
            String line = br.readLine();

            while (line != null){
                String[] pair = line.split("\t");   //split a line by tab so that pair will be word-sentiment
                Dict.put(pair[0].toLowerCase(), pair[1]);
                line = br.readLine();   //read next line
            }
            br.close();

        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {//split line into words and look up dictionary

            String line = value.toString().trim();  //read a line
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();    //we need to know which file the current line belongs to

            String[] words = line.split("\\s+");

            for (String word: words){//look up dictionary
                String sentiment = Dict.get(word.toLowerCase());    //get the sentiment of current word
                context.write(new Text(fileName + "\\s" + Dict.get(word.toLowerCase())), new IntWritable(1));    //pack up and pass
            }
        }

    }

    public static class SentimentCollection extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;

            for (IntWritable value: values){
                count += value.get();
            }
            context.write(key, new IntWritable(count));
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        configuration.set("dictionary", args[2]);   //set the third parameter: dictionary

        Job job = Job.getInstance(configuration);
        job.setJarByClass(SentimentAnalysis.class);
        job.setMapperClass(SentimentSplit.class);
        job.setReducerClass(SentimentCollection.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));   //set input as the first parameter
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //set output as the second parameter

        job.waitForCompletion(true);
    }
}
