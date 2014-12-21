LongEMA = 20:2:30;
ShortEMA = 4:10;
UpperRSI = 65:3:100;
LowerRSI = 0:3:25;
row = 1;
for i_long = 1:size(LongEMA,2)
    for i_short = 1:size(ShortEMA,2)
        for i_upper = 1:size(UpperRSI,2)
            for i_lower = 1:size(LowerRSI,2)
                result(row,:) = [LongEMA(i_long), ShortEMA(i_short), UpperRSI(i_upper), LowerRSI(i_lower)];
                row = row+1;
            end
        end
    end
end
