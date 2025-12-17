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

public class USAnnual {

    public static class Mapper3 extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private final Text outKey = new Text();
        private final DoubleWritable outVal = new DoubleWritable();

        private static final int MIN_YEAR = 1998;
        private static final int MAX_YEAR = 2025;

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.length() == 0) return;

            String[] parts = line.split("\\t");
            if (parts.length != 2) return;

            String stateYear = parts[0].trim();
            String meanStr = parts[1].trim();
            if (stateYear.length() == 0 || meanStr.length() == 0) return;

            int sep = stateYear.lastIndexOf('_');
            if (sep <= 0 || sep == stateYear.length() - 1) return;

            String yearStr = stateYear.substring(sep + 1);

            int year;
            try {
                year = Integer.parseInt(yearStr);
            } catch (NumberFormatException e) {
                return;
            }

            if (year < MIN_YEAR || year > MAX_YEAR) {
                return;  // skips the years out of range [MIN_YEAR, MAX_YEAR]
            }

            double m;
            try {
                m = Double.parseDouble(meanStr);
            } catch (NumberFormatException e) {
                return;
            }

            outKey.set(yearStr);
            outVal.set(m);
            context.write(outKey, outVal);
        }
    }

    public static class Reducer3 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

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

            double mean = sum / count;
            double rounded = Math.round(mean * 1_000_000.0) / 1_000_000.0;

            outVal.set(rounded);
            context.write(key, outVal);
        }
    }

    public static Job createJob(Configuration conf, Path input, Path output) throws IOException {
        Job job = Job.getInstance(conf, "USAnnual");

        job.setJarByClass(USAnnual.class);
        job.setMapperClass(Mapper3.class);
        job.setReducerClass(Reducer3.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }
}