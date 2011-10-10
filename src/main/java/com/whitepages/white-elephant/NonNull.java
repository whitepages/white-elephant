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
 * Generates the count of non-null records for each field in the input tuples 
   Example: A = LOAD 'mydata' as (value1: int, value2: int);
            all = GROUP A ALL;
            rates = FOREACH all GENERATE NonNull(A);
*/
public class NonNull extends EvalFunc<Tuple> implements Accumulator<Tuple>, Algebraic {
    private static TupleFactory mTupleFactory = TupleFactory.getInstance();

    @Override
    public Tuple exec(Tuple input) throws IOException {
        try {
            // the input is a tuple of a bag of tuples with some number of fields
            int[] counts = getCounts(input);
            return getCountTuple(counts);
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
                int[] counts = getCounts(input);
                return getCountTuple(counts);
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
                return combineCounts(input); 
            } catch (Exception e) {
                throw new IOException(e.getMessage());
            }
        }
    }

    // Combine the counts previously generated, output total counts 
    static public class Final extends EvalFunc<Tuple> {
        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                return combineCounts(input); 
            } catch (Exception e) {
                throw new IOException(e.getMessage());
            }
        }
    }

    // Count the number of times each field is non-null, return an array
    // counts[field index] -> count
    static protected int[] getCounts(Tuple input) throws ExecException {
        int[] counts = null;
        // the input is a tuple of a bag of tuples of ints
        DataBag bag = (DataBag)input.get(0);
        Iterator it = bag.iterator();

        // sum the count of each value
        while (it.hasNext()){
            Tuple t = (Tuple)it.next();
            if (t != null && t.size() > 0) {
                if (counts == null) {
                    counts = new int[t.size()];
                }
                for (int i = 0; i < t.size(); i++) {
                    if (t.get(i) != null) {
                        counts[i]++;
                    }
                }
            }
        }
        
        return counts;
    }
    
    static protected Tuple getCountTuple(int[] counts) {
        Tuple rez = mTupleFactory.newTuple();
        for (int i = 0; i < counts.length; i++) {
            rez.append(counts[i]);
        }
        return rez;
    }

    // Combine the intermediate counts to a total count, a tuple of field counts
    static protected Tuple combineCounts(Tuple input) throws ExecException {
        // the input is a tuple of a bag of count tuples 
        DataBag bag = (DataBag)input.get(0);
        Iterator it = bag.iterator();
        Tuple aggCounts = null;

        if ( it.hasNext() ) {
            aggCounts = (Tuple)it.next();
        }

        // get each tuple of sub-counts and aggregate them
        while (it.hasNext()) {
            Tuple t = (Tuple)it.next();
            if (t != null && t.size() > 0 ) {
                for (int i = 0; i<aggCounts.size(); i++) {
                    aggCounts.set(i, (Integer)aggCounts.get(i) + (Integer)t.get(i));
                }
            }
        }

        return aggCounts;
    }

    // Combine counts from subCounts to aggCounts
    static protected void combineCounts(int[] aggCounts, int[] subCounts) {
        for (int i = 0; i<aggCounts.length; i++) {
            aggCounts[i] += subCounts[i];
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        Schema output = new Schema();
        try {
            for ( Schema.FieldSchema inField : input.getField(0).schema.getFields() ) {
                output.add(new Schema.FieldSchema(inField.alias, DataType.INTEGER));
            }
        } catch (IOException e) {
            output.add(new Schema.FieldSchema(null, DataType.INTEGER));
        }
        return output; 
    }
    
    /* Accumulator interface implementation */

    // Accumulate the aggregated counts for the key
    // aggCounts[field index] -> count of non-nulls 
    private int[] aggCounts = null; 

    @Override
    public void accumulate(Tuple b) throws IOException {
        try {
            // first time being called for this value, so start fresh
            if ( aggCounts == null ) {
                aggCounts = getCounts(b);
            } else {    // else combine the counts
                int[] subCounts = getCounts(b);
                combineCounts(aggCounts, subCounts);
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
    public Tuple getValue() {
        return getCountTuple(aggCounts);
    }
}

