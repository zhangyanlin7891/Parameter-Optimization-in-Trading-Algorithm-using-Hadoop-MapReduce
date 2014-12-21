import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OptimizerReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	
	protected void reduce(Text token, Iterable<Text> signals, Context context) throws IOException, InterruptedException {
		
		String temp[], best_parameters = "0,0,0,0";
		double best_performance = 0;
		for (Text signal : signals){
			temp = signal.toString().split("\t");
			if (best_performance < Double.valueOf(temp[1])){
				best_performance = Double.valueOf(temp[1]);
				best_parameters = temp[0];
			}
		}
		String temp_p[] = best_parameters.split(",");
		int LongEMA = Integer.valueOf(temp_p[0]);
		int ShortEMA = Integer.valueOf(temp_p[1]);
		int UpperRSI = Integer.valueOf(temp_p[2]);
		int LowerRSI = Integer.valueOf(temp_p[3]);
		context.write(new Text("Best parameters:"), new Text("LongEMA="+LongEMA+", ShortEMA="+ShortEMA+", UpperRSI="+UpperRSI+", LowerRSI="+LowerRSI));
	}
}