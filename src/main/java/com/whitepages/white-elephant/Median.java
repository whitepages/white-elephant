package com.whitepages.whiteelephant;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Generates the median of values in bag given in first field of a tuple. 
   Example: A = LOAD 'mydata' as (key: int, value: int);
            keys = GROUP A BY key;
            medians = FOREACH keys GENERATE group AS key, MEDIAN(A.value) AS median;
*/
public class Median extends EvalFunc<Integer> implements Accumulator<Integer>, Algebraic {
    private static TupleFactory mTupleFactory = TupleFactory.getInstance();

    @Override
    public Integer exec(Tuple input) throws IOException {
        try {
            // the input is a bag of tuples of ints
            Map<String, Integer> counts = getCounts(input);

            // Iterate through to find the median
            return median(counts);

        } catch (Exception e) {
            String msg = "Error while computing median in " + this.getClass().getSimpleName();
            throw new IOException(msg + e.getMessage());
        }
    }

    // For the Algebraic interface
    @Override
    public String getInitial() {
        return Initial.class.getName();
    }

    @Override
    public String getIntermed() {
        return Intermediate.class.getName();
    }

    @Override
    public String getFinal() {
        return Final.class.getName();
    }

    // Get the counts for the initial bags passed in
    static public class Initial extends EvalFunc<Tuple> {
        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                Map<String, Integer> counts = getCounts(input);

                return TupleFactory.getInstance().newTuple(counts);
            } catch (Exception e) {
                throw new IOException(e.getMessage());
            }
        }
    }

    // Combine the counts previously generated, output intermediate counts
    static public class Intermediate extends EvalFunc<Tuple> {
        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                Map<String, Integer> aggCounts = combineCounts(input); 
                return mTupleFactory.newTuple(aggCounts);
            } catch (Exception e) {
                throw new IOException(e.getMessage());
            }
        }
    }

    // Combine the counts previously generated, output their median
    static public class Final extends EvalFunc<Integer> {
        @Override
        public Integer exec(Tuple input) throws IOException {
            try {
                Map<String,Integer> counts = combineCounts(input); 

                return median(counts);
            } catch (Exception e) {
                throw new IOException(e.getMessage());
            }
        }
    }

    // Count the number of times each value occurs, return a map
    // from value -> count
    static protected Map<String, Integer> getCounts(Tuple input) 
                    throws ExecException {
        // the input is a tuple of a bag of tuples of ints
        DataBag bag = (DataBag)input.get(0);
        Iterator it = bag.iterator();
        // want to keep sorted for finding median later
        // pig only supports String->Object maps
        Map<String, Integer> counts = new TreeMap<String, Integer>();

        // sum the count of each value
        while (it.hasNext()){
            Tuple t = (Tuple)it.next();
            if (t != null && t.size() > 0 && t.get(0) != null ) {
                String value = t.get(0).toString();
                int oldCount = counts.containsKey(value) 
                               ? counts.get(value) 
                               : 0;
                counts.put(value, oldCount+1);
            }
        }

        return counts;
    }

    // Combine the intermediate counts to a total count, a map from value -> count
    static protected Map<String,Integer> combineCounts(Tuple input) throws ExecException {
        // the input is a tuple of a bag of maps 
        DataBag bag = (DataBag)input.get(0);
        Iterator it = bag.iterator();
        Map<String, Integer> aggCounts = new TreeMap<String, Integer>();

        // get each map of sub-counts and aggregate them
        while (it.hasNext()){
            Tuple t = (Tuple)it.next();
            if (t != null && t.size() > 0 && t.get(0) != null ) {
                @SuppressWarnings("unchecked")
                Map<String,Integer> subCounts = (Map<String,Integer>)t.get(0);
                combineCountMaps(aggCounts, subCounts);
            }
        }

        return aggCounts;
    }

    // Combine counts from subCounts to aggCounts
    static protected void combineCountMaps(Map<String, Integer> aggCounts, 
                                           Map<String, Integer> subCounts) {
        for (String value : subCounts.keySet()) {
            int subCount = subCounts.get(value);
            int aggCount = aggCounts.containsKey(value) 
                            ? aggCounts.get(value) 
                            : 0;
            aggCounts.put(value, aggCount+subCount);
        }
    }

    // Return the median value from the given values 
    static protected int median(Map<String, Integer> counts) {
        long total = 0; 
        for (int count : counts.values()) {
            total += count;
        }

        Iterator<String> countedIt = counts.keySet().iterator();
        long sum = 0;
        String value = null;
        while (countedIt.hasNext() ) {
            value = countedIt.next();
            sum += counts.get(value);

            // if we hit the middle, we found it
            // if we hit the middle of an even number of them
            if (total % 2 == 0 && sum == total/2.0 && countedIt.hasNext()) {
                return (Integer.parseInt(value) + Integer.parseInt(countedIt.next())) / 2;
            } else if (sum >= total/2.0) {
                return Integer.parseInt(value);
            }
        }

        return Integer.parseInt(value);
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.INTEGER)); 
    }
    
    /* Accumulator interface implementation */

    // Accumulate the aggregated counts for the key
    // value -> count
    private Map<String, Integer> aggCounts = null; 

    @Override
    public void accumulate(Tuple b) throws IOException {
        try {
            // first time being called for this value, so start fresh
            if ( aggCounts == null ) {
                aggCounts = getCounts(b);
            } else {    // else combine the counts
                Map<String, Integer> subCounts = getCounts(b);
                combineCountMaps(aggCounts, subCounts);
            }
        } catch (Exception e) {
            throw new IOException(e.getMessage());           
        }
    }

    @Override
    public void cleanup() {
       aggCounts = null; 
    }

    @Override
    public Integer getValue() {
        return median(aggCounts);
    }
}

