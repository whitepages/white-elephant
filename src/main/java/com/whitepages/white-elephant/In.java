package com.whitepages.whiteelephant;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
   UDF used for testing membership in a set.  UDF is initialized with a type and a set.  Type can be one of:
       String
       Integer
       Long

   UDF returns Integer: 0 for FALSE, 1 for TRUE

   Examples:
       define MyStringIn com.whitepages.pigenhancements.In( 'String', 'str1', 'str2', 'str3' );
       define MyIntIn com.whitepages.pigenhancements.In( 'Integer', '5', '23', '123456', '987654321' );
 */

public class In extends EvalFunc< Integer > {
    private final static Integer
        INTEGER_TRUE  = 1,
        INTEGER_FALSE = 0;

    private final Class< ? > _class;

    private final Set< Object > _set = new HashSet< Object >();

    public In( String[] args ) throws IOException {
        if ( 2 > args.length )
            throw new IOException( "In constructor called with too few arguments" );

        try {
            String className = args[ 0 ];
            _class = Class.forName( "java.lang." + className );

            if ( String.class != _class &&
                 Integer.class != _class &&
                 Long.class != _class )
                throw new IllegalArgumentException( "Type must be one of String, Integer, or Long" );

            int length = args.length;
            Constructor< ? > constructor = _class.getConstructor( String.class );

            for ( int i = 1; i < length; i++ )
                _set.add( constructor.newInstance( args[ i ] ) );
        }
        catch ( Throwable t ) {
            throw new IOException( "Error in In constructor", t );
        }
    }

    @Override
    public Integer exec( Tuple input ) throws ExecException {
        if ( null == input || 0 == input.size() )
            return null;

        if ( 1 != input.size() )
            throw new ExecException( "In.exec(): unexpected input tuple of size " + input.size() );

        Object value = input.get( 0 );

        if ( null == value )
            return null;

        if ( !_class.isInstance( value ) )
            throw new ExecException( "In.exec(): unexpected value " + value + "; expected " + _class );

        return _set.contains( value ) ? INTEGER_TRUE : INTEGER_FALSE;
    }
    

    @Override
    public Schema outputSchema( Schema input ) {
        return new Schema( new Schema.FieldSchema( getSchemaName( "in", input ), DataType.INTEGER ) );
    }
}
