package mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: Main <inputDir> <prepOutDir> <annualOutDir> <usOutDir>");
            return 1;
        }

        Path inputDir = new Path(args[0]);
        Path prepOutDir = new Path(args[1]);
        Path annualOutDir = new Path(args[2]);
        Path usOutDir = new Path(args[3]);

        Configuration conf1 = getConf();
        Job job1 = DataPrep.createJob(conf1, inputDir, prepOutDir);
        if (!job1.waitForCompletion(true)) {
            return 1;
        }

        Configuration conf2 = new Configuration();
        Job job2 = AnnualAvarage.createJob(conf2, prepOutDir, annualOutDir);
        if (!job2.waitForCompletion(true)) {
            return 1;
        }

        Configuration conf3 = new Configuration();
        Job job3 = USAnnual.createJob(conf3, annualOutDir, usOutDir);
        if (!job3.waitForCompletion(true)) {
            return 1;
        }

        return 0;
    }
}