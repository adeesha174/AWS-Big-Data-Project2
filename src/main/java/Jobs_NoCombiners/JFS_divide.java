package Jobs_NoCombiners;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class JFS_divide {
    public static class MapperClassJFS_divide extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text ngram, Text number, Context context) throws IOException, InterruptedException {
            context.write(ngram, number);
        }
    }

    public static class ReducerClassJFS_divide extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double Tr = 1;
            double Nr = 1;

            for (Text value : values) {
                String[] split_value = value.toString().split("\\t+");
                if (split_value[1].equals("0"))
                    Tr = Long.parseLong(split_value[0]);
                else
                    Nr = Long.parseLong(split_value[0]);
            }

            double division = (Nr != 0) ? Tr / Nr : -1;
            context.write(key, new DoubleWritable(division));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "JFS_divide");
        job.setJarByClass(JFS_divide.class);

        job.setMapperClass(JFS_divide.MapperClassJFS_divide.class);
        job.setReducerClass(JFS_divide.ReducerClassJFS_divide.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
