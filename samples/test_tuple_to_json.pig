register ../target/white-elephant-0.1.0.jar
register ../lib/json-simple-1.1.jar
    
DEFINE TupleToJson com.whitepages.whiteelephant.TupleToJson();

A = LOAD 'data/test_json_data.tsv' as (name:chararray, value:int, foo:chararray);
B = FOREACH A GENERATE name, TupleToJson( 'value', value, 'foo', foo ) as json_data;

dump B;
