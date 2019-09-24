import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReducerSideJoinRunner extends Configured implements Tool {


    public static void main(String[] args) {

        try {
            int res = ToolRunner.run(new Configuration(), new ReducerSideJoinRunner(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(this.getConf(), "India Import Export Join Job");

        String[] otherArgs = new GenericOptionsParser(this.getConf(), args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: hadoop jar IndianTradeData.jar </import-data> </export-data> </output-path>");
            return 2;
        }

        job.setJarByClass(ReducerSideJoinRunner.class);

        job.setMapOutputKeyClass(CountryYearCompositeKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(CountryYearCompositeKey.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ImportMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ExportMapper.class);

        job.setReducerClass(JoinReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
