import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class StrategyRunning {
	public static void main(String[] args) throws Exception{	

		Configuration conf = new Configuration();
		conf.set("LongEMA",args[2]);
		conf.set("ShortEMA",args[3]);
		conf.set("UpperRSI",args[4]);
		conf.set("LowerRSI",args[5]);
		
		Job job = new Job(conf);
		job.setJarByClass(StrategyRunning.class);
		job.setJobName("StrategyRunning");
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(StrategyRunningMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(StrategyRunningReducer.class);
		job.setNumReduceTasks(1);
		
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.waitForCompletion(true);
	}
}
