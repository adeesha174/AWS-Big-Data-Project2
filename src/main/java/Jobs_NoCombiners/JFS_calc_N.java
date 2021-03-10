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

public class JFS_calc_N {
    enum Count_N {VALUE_N};

    public static class MapperClassJFS_N extends Mapper<Text, Text, Text, LongWritable> {
        @Override
        public void map(Text ngram, Text occurrences, Context context) throws IOException, InterruptedException {
            String[] split_value = occurrences.toString().split("\\t+");
            int sum = Integer.parseInt(split_value[0]) + Integer.parseInt(split_value[1]);
            context.write(new Text("Key"), new LongWritable(sum));
        }
    }

    public static class ReducerClassJFS_N extends Reducer<Text, LongWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new Text(Long.toString(sum)));
            context.getCounter(Count_N.VALUE_N).setValue(sum);
        }
    }

    public static class MapperClassJFS_multiply extends Mapper<Text, Text, Text, LongWritable> {
        @Override
        public void map(Text ngram, Text Nr, Context context) throws IOException, InterruptedException {
            long Nr_value = Integer.parseInt(Nr.toString().split("\\t+")[0]);
            context.write(ngram, new LongWritable(Nr_value));
        }
    }

    public static class ReducerClassJFS_multiply extends Reducer<Text, LongWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 1;
            for (LongWritable value : values) {
                sum = value.get() * context.getConfiguration().getLong("N_value", 0);
            }

            context.write(key, new Text(sum + "\t" + "1"));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // ---------------------------------- Calculating N ---------------------------------- //
        Job job1 = Job.getInstance(conf, "JFS_calc_N_1");
        job1.setJarByClass(JFS_calc_N.class);

        job1.setMapperClass(JFS_calc_N.MapperClassJFS_N.class);
        job1.setReducerClass(JFS_calc_N.ReducerClassJFS_N.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

        // ---------------------------------- Calculating result ---------------------------------- //
        long N = job1.getCounters().findCounter(Count_N.VALUE_N).getValue();
        conf.setLong("N_value", N);

        Job job2 = Job.getInstance(conf, "JFS_calc_N_2");
        job2.setJarByClass(JFS_calc_N.class);

        job2.setMapperClass(JFS_calc_N.MapperClassJFS_multiply.class);
        job2.setReducerClass(JFS_calc_N.ReducerClassJFS_multiply.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(args[3]));
        FileOutputFormat.setOutputPath(job2, new Path(args[4]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
