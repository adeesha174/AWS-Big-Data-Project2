package Others;

import java.io.IOException;
import java.lang.Math;
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

public class ReadCorpus {
    public static class MapperClass extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int random_key = (int)(Math.random() * 24);
            context.write(new LongWritable(random_key), value);
        }
    }
    public static class ReducerClass extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Corpus reader");
        job.setJarByClass(ReadCorpus.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setNumReduceTasks(24);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
