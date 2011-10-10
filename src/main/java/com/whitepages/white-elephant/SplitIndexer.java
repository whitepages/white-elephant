package com.whitepages.whiteelephant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Takes a string as input and produces a bad of tuples of two values:
 *    A substring from the input as a result of a String.split()
 *    A 0 based index
 *
 * The constructor takes a split() regex.
 *
 * Example:
 *     Given construction SplitIndexer( ';\\s*' )
 *
 *     SplitIndexer( 'Visa; MasterCard;Amex' )
 *           produces
 *     ( { 'Visa', 0 }, { 'MasterCard', 1 }, { 'Amex', 2 } )
 */

public class SplitIndexer extends EvalFunc< DataBag > {
    // ********** Class Variables **********
    private static final BagFactory BAG_FACTORY = BagFactory.getInstance();

    private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

    // ********** Instance Variables **********
    private final String _regex;

    // ********** Constructor **********
    public SplitIndexer( String regex ) {
        _regex = regex;
    }

    // ********** EvalFunc Methods **********
    @Override
    public DataBag exec( Tuple input ) throws IOException {
        if ( null == input || 1 != input.size() )
            throw new IOException( "Too many arguments passed to SplitIndexer: " + input.size() );

        String originalString = ( String )input.get( 0 );

        String[] splitElements = originalString.split( _regex );

        DataBag result = BAG_FACTORY.newDefaultBag();

        int i = 0;

        for ( String splitElement : splitElements ) {
            Tuple newTuple = TUPLE_FACTORY.newTuple( 2 );

            newTuple.set( 0, splitElement );
            newTuple.set( 1, i++ );

            result.add( newTuple );
        }

        return result;
    }


    @Override
    public Schema outputSchema( Schema input ) {
        List< Schema.FieldSchema > fields = new ArrayList< Schema.FieldSchema >( 2 );
        fields.add( new Schema.FieldSchema( "element", DataType.CHARARRAY ) );
        fields.add( new Schema.FieldSchema( "rank", DataType.INTEGER ) );

        return new Schema( fields );
    }
}
