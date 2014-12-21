import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import au.com.bytecode.opencsv.CSVParser;

public class OptimizerMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		String input_path = conf.get("Input");
		String inter_path = conf.get("Inter");
		
		String[] lines = new CSVParser().parseLine(value.toString());
		
		String output_path = inter_path+"/Output-"+lines[0]+"-"+lines[1]+"-"+lines[2]+"-"+lines[3];
		String para[] = {input_path, output_path, lines[0], lines[1], lines[2], lines[3]};
		
		try {
			StrategyRunning.main(para);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String result_path = output_path+"/part-r-00000";		//The output of inner MapReduce
		Configuration conf_f = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(result_path), conf_f);
		FSDataInputStream hdfsInStream = fs.open(new Path(result_path));
		int result_length = hdfsInStream.available();
		byte result_by[] = new byte[result_length];
		hdfsInStream.read(result_by);
		String result_str = new String(result_by,"UTF-8");

		context.write(new Text("Parameters-Performance"), new Text(result_str));
	}
}
