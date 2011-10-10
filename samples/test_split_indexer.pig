register ../target/white-elephant-0.1.0.jar

define SplitIndexer com.whitepages.whiteelephant.SplitIndexer( ';\\s*' );

A = LOAD 'data/test_splitter.tsv' as (compound:chararray);
B = FOREACH A GENERATE SplitIndexer( compound );

DUMP B;

