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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataPrep {

    public static class Mapper1 extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private String year;
        private Text outKey = new Text();
        private DoubleWritable outVal = new DoubleWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            String name = split.getPath().getName(); // e.g., daily_88101_2024.csv

            String y = "0000";
            String[] parts = name.split("_");
            if (parts.length >= 3) {
                String last = parts[2];          // "2024.csv"
                int dot = last.indexOf('.');
                if (dot > 0) {
                    y = last.substring(0, dot);  // "2024"
                } else {
                    y = last;
                }
            }

            year = y;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Parser.ParsedLine rec = Parser.parse(value.toString());
            if (rec == null) {
                return;
            }

            
            String k = rec.stateCode + "_" + year;
            outKey.set(k);
            outVal.set(rec.dailyMean);
            context.write(outKey, outVal);
        }
    }

    public static class Reducer1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            for (DoubleWritable v : values) {
                context.write(key, v);
            }
        }
    }

    public static Job createJob(Configuration conf, Path input, Path output) throws IOException {
        Job job = Job.getInstance(conf, "DataPrep");

        job.setJarByClass(DataPrep.class);
        job.setInputFormatClass(CSVFileInputFormat.class);

        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }
}