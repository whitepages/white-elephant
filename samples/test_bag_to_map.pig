register ../target/white-elephant-0.1.0.jar
DEFINE BagToMap com.whitepages.whiteelephant.BagToMap();

A = LOAD 'data/test_data.tsv' as (name:chararray, value:int, title:chararray);
B = GROUP A BY ( name );
C = FOREACH B GENERATE group as name:chararray, BagToMap( A.title, A.value );

dump C;