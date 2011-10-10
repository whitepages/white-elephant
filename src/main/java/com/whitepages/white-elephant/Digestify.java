package com.whitepages.whiteelephant;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class Digestify extends EvalFunc< String > {
    // ******************** Class Constants ********************
    public final static String DEFAULT_ALGORITHM = "SHA";

    // ******************** Instance Vars ********************
    private final MessageDigest _md;

    // ******************** Constructors ********************
    public Digestify() throws IOException {
        this( DEFAULT_ALGORITHM );
    }

    public Digestify( String algorithm ) throws IOException {
        try {
            _md = MessageDigest.getInstance( algorithm );
        }
        catch ( NoSuchAlgorithmException e ) {
            throw new IOException( e );
        }
    }

    // ******************** Instance Methods ********************
    
	/**
	 * @return Message Digest in String format
	 */
	@Override
	public String exec( Tuple input ) throws IOException {
        if ( null == input )
            throw new IOException( "Expected tuple as input, got null" );

        int size = input.size();
        StringBuilder sb = new StringBuilder();


        for ( int i = 0; i < size; i++ ) {
            Object element = input.get( i );
            String s = null == element ? "" : element.toString().trim();

            if ( 0 < i )
                sb.append( '\t' );

            sb.append( s );
        }


        _md.reset();
        byte[] digest = _md.digest( sb.toString().getBytes() );

        // convert byte array to hexified string
        sb.setLength( 0 );

        for ( byte b : digest )
            sb.append( Integer.toHexString( 0xFF & b ) );

        return sb.toString();
	}

	/**
	 * @return Schema describing output of this UDF containing:
	 * <ul>
	 * <li>default name = "digest"
	 * <li>type = "chararray"
	 * </ul>
	 */
	@Override
	public Schema outputSchema( Schema input ) {
        return new Schema( new Schema.FieldSchema( "digest", DataType.CHARARRAY ) );
	}
}
