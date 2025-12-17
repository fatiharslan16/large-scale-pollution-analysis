package mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnnualAvarage {

    public static class Mapper2 extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private final Text outKey = new Text();
        private final DoubleWritable outVal = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.length() == 0) return;

            // input from DataPrep: state_year \t dailyMean
            String[] parts = line.split("\\t");
            if (parts.length != 2) return;

            String k = parts[0].trim();
            String meanStr = parts[1].trim();
            if (k.length() == 0 || meanStr.length() == 0) return;

            double m;
            try {
                m = Double.parseDouble(meanStr);
            } catch (NumberFormatException e) {
                return;
            }

            outKey.set(k);
            outVal.set(m);
            context.write(outKey, outVal);
        }
    }

    public static class Reducer2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private final DoubleWritable outVal = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;
            int count = 0;

            for (DoubleWritable v : values) {
                sum += v.get();
                count++;
            }

            if (count == 0) return;

            double annualMean = sum / count;

            // round to 6 decimals
            double roundedAnnualMean =
                    Math.round(annualMean * 1_000_000.0) / 1_000_000.0;

            outVal.set(roundedAnnualMean);
            context.write(key, outVal);
        }
    }

    public static Job createJob(Configuration conf, Path input, Path output) throws IOException {
        Job job = Job.getInstance(conf, "AnnualAvarage");

        job.setJarByClass(AnnualAvarage.class);

        job.setMapperClass(Mapper2.class);
        job.setReducerClass(Reducer2.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }
}