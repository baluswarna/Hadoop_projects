/* wc.pig */
set io.sort.mb 10;
A = LOAD 'hdfs:///pigdata/' USING PigStorage(',','-tagsource');  -- load the csv file 
B = FILTER A BY $1 neq 'Date';
format = foreach B generate $0,$1,SUBSTRING((chararray)$1,8,10),(float)$7;
groupbymonth = group format by ($0,SUBSTRING((chararray)$1,(int)0,(int)7));
sorted = foreach groupbymonth {
         order_asc = order format by $2 asc;
        monthbegin    = limit order_asc 1;
           order_dsc = order format by $2 desc;
        monthend    = limit order_dsc 1;
 
          generate flatten(monthend),flatten(monthbegin);
}
xi = foreach sorted generate $0,SUBSTRING((chararray)$1,(int)0,(int)7), (float)(($3-$7)/($7));
xi_new = foreach xi generate $0 as fname,$1 as month,$2 as rr;
J = GROUP xi_new BY $0 ;
xiavg = foreach J generate group as id ,AVG(xi_new.$2) as avg ;
L = JOIN xi_new BY $0, xiavg BY $0;
C = foreach L generate $0,$1, ($2-$4)*($2-$4);
D = group C by $0;
sigma = foreach D generate group, SUM(C.$2);
countmonths = foreach D generate group, (float)(COUNT(C)-1);
combine = join sigma by $0, countmonths by $0;
volatility = foreach combine generate $0, (float)SQRT($1/$3);
nozeroes = filter volatility by $1!= 0.0;
order_asc = order nozeroes by $1 asc;
top10min = limit order_asc 10;
order_dsc = order nozeroes by $1 desc;
top10max = limit order_dsc 10;
final = union top10min,top10max;
store final into 'hdfs:///pigdata/hw3_out'; 