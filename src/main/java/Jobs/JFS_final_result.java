package Jobs;

import Utils.New_key;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class JFS_final_result {
    public static class MapperClassJFS_final_result extends Mapper<Text, DoubleWritable, New_key, Text> {
        @Override
        public void map(Text ngram, DoubleWritable probability, Context context) throws IOException, InterruptedException {
            String[] tokens = ngram.toString().split(" ");
            if (tokens.length == 3) {
                context.write(new New_key(tokens[0], tokens[1], probability.get()), new Text(tokens[2]));
            }
        }
    }

    public static class ReducerClassJFS_final_result extends Reducer<New_key, Text, Text, DoubleWritable> {
        @Override
        public void reduce(New_key key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String prefix = key.getW1() + " " + key.getW2();
            double probability = key.getProbability();

            for (Text value : values) {
                String new_key = prefix + " " + value.toString();
                context.write(new Text(new_key), new DoubleWritable(probability));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "JFS_final_result");
        job.setJarByClass(JFS_final_result.class);

        job.setMapperClass(JFS_final_result.MapperClassJFS_final_result.class);
        job.setReducerClass(JFS_final_result.ReducerClassJFS_final_result.class);

        job.setMapOutputKeyClass(New_key.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
