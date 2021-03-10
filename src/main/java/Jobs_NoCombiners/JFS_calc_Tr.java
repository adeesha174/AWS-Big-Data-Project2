package Jobs_NoCombiners;

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
import java.util.ArrayList;
import java.util.List;

public class JFS_calc_Tr {
    public static class MapperClassJFS_Tr01 extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text ngram, Text value, Context context) throws IOException, InterruptedException {
            String[] split_value = value.toString().split("\\t+");
            Text new_key = new Text(split_value[0]);
            Text new_value = new Text(ngram.toString() + "\t" + split_value[1]);
            context.write(new_key, new_value);
        }
    }

    public static class MapperClassJFS_Tr10 extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text ngram, Text value, Context context) throws IOException, InterruptedException {
            String[] split_value = value.toString().split("\\t+");
            Text new_key = new Text(split_value[1]);
            Text new_value = new Text(ngram.toString() + "\t" + split_value[0]);
            context.write(new_key, new_value);
        }
    }

    public static class ReducerClassJFS_Tr extends Reducer<Text, Text, Text, LongWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            List<String> ngrams = new ArrayList<>();
            
            for (Text value : values) {
                String[] split_value = value.toString().split("\\t+");
                ngrams.add(split_value[0]);
                sum += Integer.parseInt(split_value[1]);
            }
            
            for (String ngram : ngrams) {
                context.write(new Text(ngram), new LongWritable(sum));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // ---------------------------------- Calculating Tr01 ---------------------------------- //
        Job job1 = Job.getInstance(conf, "JFS_calc_Tr01");
        job1.setJarByClass(JFS_calc_Tr.class);

        job1.setMapperClass(JFS_calc_Tr.MapperClassJFS_Tr01.class);
        job1.setReducerClass(JFS_calc_Tr.ReducerClassJFS_Tr.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

        // ---------------------------------- Calculating Tr10 ---------------------------------- //
        Job job2 = Job.getInstance(conf, "JFS_calc_Tr10");
        job2.setJarByClass(JFS_calc_Tr.class);

        job2.setMapperClass(JFS_calc_Tr.MapperClassJFS_Tr10.class);
        job2.setReducerClass(JFS_calc_Tr.ReducerClassJFS_Tr.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
