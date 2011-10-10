package com.whitepages.whiteelephant;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * A Pig UDF (User Defined Function) which generates the String form of a postgres compatible
 * timestamp.  String is of the form "YYYY-MM-DD HH:MM:SS".
 * This UDF presumes to take no input and produces a timestamp representing now.  Timezone is assumed
 * to be local.
 */
public class Now extends EvalFunc<String> {
    private final static String DEFAULT_FORMAT_STRING = "yyyy-MM-dd HH:mm:ss";

    private final SimpleDateFormat _formatter;

	/**
	 * Default constructor produces output compatible with Postgres's timestamp column type.
	 */
	public Now() {
        this( DEFAULT_FORMAT_STRING );
	}

    /**
     * This constructor takes a SimpleDateFormat pattern.
     *
     * @see java.text.SimpleDateFormat
     */
    public Now( String formatString ) {
        _formatter = new SimpleDateFormat( formatString );
    }

    

	/**
	 * @return UUID in String format
	 */
	public String exec(Tuple input) {
		// We really want an empty input Tuple, but pig insists on trying
		// to give us something useful.  When no arguments are supplied
		// to our UDF, pig passes the entire active Tuple.

        return _formatter.format( new Date( System.currentTimeMillis() ) );
	}

	/**
	 * @return Schema describing output of this UDF containing:
	 * <ul>
	 * <li>default name = "guid"
	 * <li>type = "chararray"
	 * </ul>
	 */
	@Override
	public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema("timestamp", DataType.CHARARRAY));
	}
}
