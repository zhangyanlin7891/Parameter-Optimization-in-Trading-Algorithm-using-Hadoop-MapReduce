import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Optimizer {
	public static void main(String[] args) throws Exception{	//0-input file, 1-parameter file, 2-intermediate path, 3-output file
		
		Configuration conf = new Configuration();
		conf.set("Input",args[0]);
		conf.set("Inter",args[2]);
//		conf.setLong("mapreduce.input.fileinputformat.split.maxsize",4915);	//48kB parameters into 20 splits
		
		Job job = new Job(conf);
		job.setJarByClass(Optimizer.class);
		job.setJobName("Optimizer");
		
		TextInputFormat.addInputPath(job, new Path(args[1]));
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(OptimizerMapper.class);
		job.setReducerClass(OptimizerReducer.class);
		job.setNumReduceTasks(1);
		
		TextOutputFormat.setOutputPath(job, new Path(args[3]));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.waitForCompletion(true);
	}

}
