register ../target/white-elephant-0.1.0.jar
define Digestify com.whitepages.whiteelephant.Digestify();

A = LOAD 'data/cass_small.tsv' as (input_addr:chararray, input_addr2:chararray, input_location:chararray);
B = FOREACH A GENERATE *,Digestify(*);

DUMP B;
DESCRIBE B;
