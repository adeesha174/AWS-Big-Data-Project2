package Jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JFS_calc_r {
    public static class MapperClassJFS_r extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] tempLine = line.toString().split("\\t+");
            Text ngram = new Text(tempLine[0]);
            String occurrences = tempLine[2];
            // Tagging the records according to the different parts of the corpus
            if (lineId.get() >= 0 && lineId.get() <= 11) // First part of the corpus
                context.write(ngram, new Text(occurrences + "\t" + "0"));
            else // Second part of the corpus
                context.write(ngram, new Text(occurrences + "\t" + "1"));
        }
    }

    public static class CombinerClassJFS_r extends Reducer<Text, Text, Text, Text> {
        @Override
            public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                int sum_r0 = 0;
                int sum_r1 = 0;

                for (Text value : values) {
                    String[] split_value = value.toString().split("\\t+");
                    if (split_value[1].equals("0"))
                        sum_r0 += Integer.parseInt(split_value[0]);
                    else
                        sum_r1 += Integer.parseInt(split_value[0]);
                }

                String intermediate_str = sum_r0 + "\t" + sum_r1;
                context.write(key, new Text(intermediate_str));
            }
        }

    public static class ReducerClassJFS_r extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum_r0 = 0;
            int sum_r1 = 0;

            for (Text value : values) {
                String[] split_value = value.toString().split("\\t+");
                sum_r0 += Integer.parseInt(split_value[0]);
                sum_r1 += Integer.parseInt(split_value[1]);
            }

            String final_str = sum_r0 + "\t" + sum_r1;
            context.write(key, new Text(final_str));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "JFS_calc_r");
        job.setJarByClass(JFS_calc_r.class);

        job.setMapperClass(JFS_calc_r.MapperClassJFS_r.class);
        job.setCombinerClass(CombinerClassJFS_r.class);
        job.setReducerClass(JFS_calc_r.ReducerClassJFS_r.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
