register ../target/white-elephant-0.1.0.jar
define IN_STRING_SET com.whitepages.whiteelephant.In( 'String', 'bb', 'cc', 'x', 'y', 'z' );
define IN_INT_SET com.whitepages.whiteelephant.In( 'Integer', '4', '8', '10' );

A = LOAD 'data/test_data.tsv' as (name:chararray, value:int, foo:chararray);

B = FOREACH A GENERATE name, IN_STRING_SET( name );
C = FOREACH A GENERATE value, IN_INT_SET( value );

DUMP B;
DUMP C;
