import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class StrategyRunningReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	@Override
	
	protected void reduce(Text token, Iterable<Text> signals, Context context) throws IOException, InterruptedException {
		
		//Use treemap to store the trading signals, using time as the key
		Map<Integer, double[]> actions = new TreeMap<Integer, double[]>();
		//Read out all the signals
		String temp[];
		int time = 0,last_time = 0;
		double last_closed = 0;
		double price_action[] = new double[2];
		for (Text signal : signals){
			temp = signal.toString().split(",");
			time = Integer.parseInt(temp[0]);
			price_action[0] = Double.valueOf(temp[1]);
			price_action[1] = Double.valueOf(temp[2]);
			if (price_action[1] == 2){						//Last day's closing price, for evaluation only
				last_time = time;
				last_closed = price_action[0];
			}
			else{
				if (actions.containsKey(time)){				//There's already action for this day
					double existed[] = actions.get(time);
					if (existed[1] != price_action[1])		//It's a signal on the opposite position
						actions.remove(time);				//Remove it (one buy and one sell action)
				}											//If the signal is on the same position, ignore it
				actions.put(time, price_action);
			}
		}
		
		//Measure the performance of the trading signals
		//Assume that each action just takes volume 1
		int position = 0;
		double money = 0;
		Iterator it = actions.keySet().iterator();
		while (it.hasNext()){
			price_action = actions.get(it.next());
			if (price_action[1] == 0){		//Sell signal
				position = position-1;
				money = money+price_action[0];
			}
			else{
				position = position+1;
				money = money-price_action[0];
			}
		}
		//Calculate the final profit
		double profit = money+position*last_closed;
		
		context.write(token, new DoubleWritable(profit));
	}
}
