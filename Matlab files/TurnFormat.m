result = zeros(10,910);
row = 1;
column = 1;
count = 0;
for i = 1:size(m,1)
    result(row,column:column+6) = m(i,:);
    column = column+7;
    count = count+1;
    if count == 100
        for j = 1:30
            result(row,column:column+6) = m(i+j,:);
            column = column+7;
        end
        count = 0;
        column = 1;
        row = row+1;
    end
end