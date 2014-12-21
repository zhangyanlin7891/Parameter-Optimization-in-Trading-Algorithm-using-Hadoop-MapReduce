import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

import au.com.bytecode.opencsv.CSVParser;

public class StrategyRunningMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//Read out the parameters for the algorithm
		Configuration conf = context.getConfiguration();
		int LongEMA = Integer.parseInt(conf.get("LongEMA"));
		int ShortEMA = Integer.parseInt(conf.get("ShortEMA"));
		int UpperRSI = Integer.parseInt(conf.get("UpperRSI"));
		int LowerRSI = Integer.parseInt(conf.get("LowerRSI"));
		int RSI_length = 14;
		
		//Key for write
		String key_write = LongEMA+","+ShortEMA+","+UpperRSI+","+LowerRSI;
		
		//Read out the data
		String[] lines = new CSVParser().parseLine(value.toString());
		//[0]-time,[1]-open,[2]-high,[3]-low,[4]-close,[5]-volume,[6]-openint
		//Totally data of 130 days in each line
		
		//Partition the data into arrays
		boolean last = false;
		int last_length = 0;
		int time[] = new int[130];
		double open[] = new double[130];
		double close[] = new double[130];
		for (int i = 1; i <= 130; i++){
			if (i*7 > lines.length){	//The last line of the data has less than 130 days of data
				last = true;
				last_length = i-1;
				break;
			}
			time[i-1] = Integer.parseInt(lines[(i-1)*7]);
			open[i-1] = Double.valueOf(lines[(i-1)*7+1]);
			close[i-1] = Double.valueOf(lines[(i-1)*7+4]);
		}
		if (last){
			int i_temp[] = Arrays.copyOf(time, last_length);
			time = i_temp;
			double d_temp[] = Arrays.copyOf(open, last_length);
			open = d_temp;
			d_temp = Arrays.copyOf(close, last_length);
			close = d_temp;
		}
		
		//Generate trading signals
		//For decision, 1:buy, 0:sell, 2:last-day price (for evaluating profit)
		//<Key,value> pair, value: "time,price,position"
		
		//MCAD rule
		double alpha_long = 2/(LongEMA+1);
		double alpha_short = 2/(ShortEMA+1);
			//Initial calculation
		double EMA_L = close[0];
		double EMA_S = close[0];
		for (int i = 1; i<LongEMA; i++){
			EMA_L = alpha_long*close[i]+(1-alpha_long)*EMA_L;
			EMA_S = alpha_short*close[i]+(1-alpha_short)*EMA_S;
		}
			//Continuous calculation and finding the signals
		boolean LhighSlow = EMA_L>EMA_S;
		for (int i = LongEMA; i<time.length; i++){
			EMA_L = alpha_long*close[i]+(1-alpha_long)*EMA_L;
			EMA_S = alpha_short*close[i]+(1-alpha_short)*EMA_S;
			if (LhighSlow != EMA_L>EMA_S){
				LhighSlow = EMA_L>EMA_S;
				if (LhighSlow)						//Sell short
					context.write(new Text(key_write), new Text(time[i]+","+close[i]+",0"));
				else								//Buy long
					context.write(new Text(key_write), new Text(time[i]+","+close[i]+",1"));
			}
		}
		
		//RSI rule
			//The daily change
		double change[] = new double[time.length];
		change[0] = 0;
		for (int i = 1; i < time.length; i++)
			change[i] = close[i] - close[i-1];
			//Initiate the start period
		double RSI[] = new double[time.length];
		for (int i = 0; i < RSI_length; i++)
			RSI[i] = 50;
			//Calculate the first meaningful RSI value
		double up = 0, down = 0;
		for (int i = 1; i<=RSI_length; i++){
			if (change[i] >= 0)
				up = up+change[i];
			else
				down = down-change[i];
		}
		RSI[RSI_length] = 100-100/(1+up/down);
			//Calculate the RSIs
		for (int i = RSI_length+1; i < time.length; i++){
			if (change[i-RSI_length] >= 0)
				up = up-change[i-RSI_length];
			else
				down = down+change[i-RSI_length];
			if (change[i] >= 0)
				up = up+change[i];
			else
				down = down-change[i];
			RSI[i] = 100-100/(1+up/down);
		}
			//Find the signals
		boolean higher = RSI[RSI_length]>UpperRSI, lower = RSI[RSI_length]<LowerRSI;
		for (int i = RSI_length+1; i < time.length; i++){
			if (RSI[i] > UpperRSI){
				higher = true;
				lower = false;
			}
			else if (RSI[i] < LowerRSI){
				higher = false;
				lower = true;
			}
			else{
				if (higher)			//Sell short
					context.write(new Text(key_write), new Text(time[i]+","+close[i]+",0"));
				if (lower)			//Buy long
					context.write(new Text(key_write), new Text(time[i]+","+close[i]+",1"));
				higher = false;
				lower = false;
			}
		}
		
		//Write down the last-day price
		context.write(new Text(key_write), new Text(time[time.length-1]+","+close[time.length-1]+",2"));
	}
}