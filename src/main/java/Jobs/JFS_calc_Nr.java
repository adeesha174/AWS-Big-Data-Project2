package Jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class JFS_calc_Nr {
    public static class MapperClassJFS_Nr0 extends Mapper<Text, Text, LongWritable, Text> {
        @Override
        public void map(Text ngram, Text occurrences, Context context) throws IOException, InterruptedException {
            String[] split_occurrences = occurrences.toString().split("\\t+");
            int new_occurrences = Integer.parseInt(split_occurrences[0]);
            context.write(new LongWritable(new_occurrences), ngram);
        }
    }

    public static class MapperClassJFS_Nr1 extends Mapper<Text, Text, LongWritable, Text> {
        @Override
        public void map(Text ngram, Text occurrences, Context context) throws IOException, InterruptedException {
            String[] split_occurrences = occurrences.toString().split("\\t+");
            int new_occurrences = Integer.parseInt(split_occurrences[1]);
            context.write(new LongWritable(new_occurrences), ngram);
        }
    }

    public static class CombinerClassJFS_Nr extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder all_ngrams = new StringBuilder();

            for(Text value : values) {
                all_ngrams.append(value).append("\t");
            }

            String all_ngrams_str = all_ngrams.substring(0, all_ngrams.length() - 1);
            context.write(key, new Text(all_ngrams_str));
        }
    }

    public static class ReducerClassJFS_Nr extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int values_size = 0;
            StringBuilder all_values = new StringBuilder();

            for(Text value : values) {
                String[] split_value = value.toString().split("\\t+");
                values_size += split_value.length;
                all_values.append(value).append("\t");
            }

            String all_values_str = all_values.substring(0, all_values.length() - 1);
            context.write(new LongWritable(values_size), new Text(all_values_str));
        }
    }

    public static class MapperClassJFS_Nr_b extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        public void map(LongWritable values_size, Text ngrams, Context context) throws IOException, InterruptedException {
            String[] ngrams_separated = ngrams.toString().split("\\t+");
            for (String ngram : ngrams_separated)
                context.write(new Text(ngram), values_size);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // ---------------------------------- Calculating Nr_a ---------------------------------- //
        Job job1 = Job.getInstance(conf, "JFS_calc_Nr_a");
        job1.setJarByClass(JFS_calc_Nr.class);

        job1.setMapperClass(args[1].equals("0") ? JFS_calc_Nr.MapperClassJFS_Nr0.class : JFS_calc_Nr.MapperClassJFS_Nr1.class);
        job1.setCombinerClass(JFS_calc_Nr.CombinerClassJFS_Nr.class);
        job1.setReducerClass(JFS_calc_Nr.ReducerClassJFS_Nr.class);

        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Text.class);

        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[2])); // r0 and r1
        FileOutputFormat.setOutputPath(job1, new Path(args[3])); // Intermediate path
        job1.waitForCompletion(true);

        // ---------------------------------- Calculating Nr_b ---------------------------------- //
        Job job2 = Job.getInstance(conf, "JFS_calc_Nr_b");
        job2.setJarByClass(JFS_calc_Nr.class);

        job2.setMapperClass(JFS_calc_Nr.MapperClassJFS_Nr_b.class);

        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        job2.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job2, new Path(args[3])); // Intermediate path
        FileOutputFormat.setOutputPath(job2, new Path(args[4])); // Output path
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
