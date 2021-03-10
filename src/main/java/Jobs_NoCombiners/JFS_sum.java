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

public class JFS_sum {
    public static class MapperClassJFS_sum extends Mapper<Text, LongWritable, Text, LongWritable> {
        @Override
        public void map(Text ngram, LongWritable Nr, Context context) throws IOException, InterruptedException {
            context.write(ngram, Nr);
        }
    }

    public static class ReducerClassJFS_sum_0 extends Reducer<Text, LongWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }

            String result = sum + "\t" + "0";
            context.write(key, new Text(result));
        }
    }

    public static class ReducerClassJFS_sum_1 extends Reducer<Text, LongWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }

            String result = sum + "\t" + "1";
            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // ---------------------------------- Calculating Tr01 + tr10 ---------------------------------- //
        Job job1 = Job.getInstance(conf, "JFS_sum_Tr");
        job1.setJarByClass(JFS_sum.class);

        job1.setMapperClass(JFS_sum.MapperClassJFS_sum.class);
        job1.setReducerClass(JFS_sum.ReducerClassJFS_sum_0.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileInputFormat.addInputPath(job1, new Path(args[2]));
        FileOutputFormat.setOutputPath(job1, new Path(args[3]));
        job1.waitForCompletion(true);

        // ---------------------------------- Calculating Nr0 + Nr1 ---------------------------------- //
        Job job2 = Job.getInstance(conf, "JFS_sum_Nr");
        job2.setJarByClass(JFS_sum.class);

        job2.setMapperClass(JFS_sum.MapperClassJFS_sum.class);
        job2.setReducerClass(JFS_sum.ReducerClassJFS_sum_1.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(args[4]));
        FileInputFormat.addInputPath(job2, new Path(args[5]));
        FileOutputFormat.setOutputPath(job2, new Path(args[6]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
