/**
 * Created by aliHitawala on 9/24/16.
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.mapred.Reducer;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;

public class AnagramSorter {

    public static class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private Text sortedText = new Text();
        private Text orginalText = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter)
                throws IOException {
            String word = value.toString();
            char[] wordChars = word.toCharArray();
            Arrays.sort(wordChars);
            String sortedWord = new String(wordChars);
            sortedText.set(sortedWord);
            orginalText.set(word);
            outputCollector.collect(sortedText, orginalText);
        }
    }

    public static class WordCountReducer extends MapReduceBase implements Reducer<Text, Text, IntWritable, Text> {
        private IntWritable outputKey = new IntWritable();
        private Text outputValue = new Text();

        public void reduce(Text anagramKey, Iterator<Text> anagramValues,
                           OutputCollector<IntWritable, Text> results, Reporter reporter)
                throws IOException {
            StringBuilder output = new StringBuilder("");
            int count = 0;
            while (anagramValues.hasNext()) {
                Text anagam = anagramValues.next();
                output.append(anagam.toString()).append(",");
                count++;
            }
            // key is negative to sort in decending order in terms of number of that different anagrams
            outputKey.set(-count);
            // output value is comma separated set of anagrams
            outputValue.set(output.toString());
            results.collect(outputKey, outputValue);
        }
    }

    public static class AnagramMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
        private IntWritable outputKey = new IntWritable();
        private Text outputValue = new Text();

        public void map(LongWritable key, Text value,
                        OutputCollector<IntWritable, Text> outputCollector, Reporter reporter)
                throws IOException {
            //value will be of the form <-1 ali> and <-3 ola,alo,loa>
            StringTokenizer outputTokenizer = new StringTokenizer(value.toString());
            int k = Integer.parseInt(outputTokenizer.nextToken());
            String v = outputTokenizer.nextToken();
            outputKey.set(k);
            outputValue.set(v);
            outputCollector.collect(outputKey, outputValue);
        }
    }

    public static class AnagramReducer extends MapReduceBase implements Reducer<IntWritable, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();
        public void reduce(IntWritable anagramKey, Iterator<Text> anagramValues,
                           OutputCollector<Text, Text> results, Reporter reporter)
                throws IOException {
            StringBuilder output = new StringBuilder();
            while (anagramValues.hasNext()) {
                Text anagam = anagramValues.next();
                output.append(anagam.toString().replace(",", " "));
                if (anagramValues.hasNext())
                    output.append("\n");
            }
            outputKey.set(output.toString());
            outputValue.set(String.valueOf(""));
            results.collect(outputKey, outputValue);
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(AnagramSorter.class);
        conf.setJobName("wordCount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(WordCountMapper.class);
        conf.setReducerClass(WordCountReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        long unixTime = System.currentTimeMillis() / 1000L;
        String temp_file = "temp_"+unixTime;
        FileOutputFormat.setOutputPath(conf, new Path(temp_file));
        JobConf conf2 = new JobConf(AnagramSorter.class);
        conf2.setJobName("anagramMergerSorter");

        conf2.setOutputKeyClass(Text.class);
        conf2.setOutputValueClass(Text.class);

        conf2.setMapperClass(AnagramMapper.class);
        conf2.setReducerClass(AnagramReducer.class);
        conf2.setMapOutputKeyClass(IntWritable.class);
        conf2.setMapOutputValueClass(Text.class);
        conf2.setInputFormat(TextInputFormat.class);
        conf2.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf2, new Path(temp_file));
        FileOutputFormat.setOutputPath(conf2, new Path(args[1]));

        // chaining jobs
        Job job1 = new Job(conf);
        Job job2 = new Job(conf2);
        JobControl jobControl = new JobControl("JobController");
        jobControl.addJob(job1);
        jobControl.addJob(job2);
        job2.addDependingJob(job1);
        jobControl.run();
    }
}
