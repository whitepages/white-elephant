package com.whitepages.whiteelephant;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * A Pig UDF (User Defined Function) which generates a String to Object map from
 * an input set of tuples
*/

public class BagToMap extends EvalFunc<Map<String,Object>> {
    /**
     * @return map of data from the Tuple input
     */
    public Map<String,Object> exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1 || input.get(0) == null)
            return null;

        try{
            Map<String, Object> mapData = new HashMap<String, Object>();
            
            DataBag keys = (DataBag)input.get(0);
            DataBag values = (DataBag)input.get(1);

            if( keys.size() != values.size() ){
                throw new RuntimeException( "Key bag and Value bag are of different sizes" );
            }
            
            Iterator<Tuple> itKeys = keys.iterator();
            Iterator<Tuple> itValue = values.iterator();
            
            while( itKeys.hasNext() && itValue.hasNext() ) {
                Tuple keyTuple = itKeys.next();
                String key = (String)keyTuple.get(0);

                Tuple valueTuple = itValue.next();
                Object value = (Object)valueTuple.get(0);
                
                mapData.put( key, value );
            }
            return mapData;
        } catch(Exception e) {
            log.warn("Failed to translate bag to map; error - " + e.toString());
            e.printStackTrace();
            return null;
        }
    }
    
    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.MAP));
    }
}
