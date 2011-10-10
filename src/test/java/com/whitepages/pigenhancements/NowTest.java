package com.whitepages.whiteelephant;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import junit.framework.TestCase;
import org.apache.pig.data.Tuple;
import org.junit.Test;


public class NowTest extends TestCase {
    private void _helper( Now evalFunc, String parseString, long roundErrorMillis ) throws Exception {
        long   start  = System.currentTimeMillis();
        String output = evalFunc.exec( ( Tuple )null );
        long   end    = System.currentTimeMillis();

		assertNotNull( "Now.exec() produced null output", output );

        SimpleDateFormat sdf = new SimpleDateFormat( parseString );
        
        if ( 0L != roundErrorMillis )
            start -=  start % roundErrorMillis;

        try {
            Date date = sdf.parse( output );
            long timestamp = date.getTime();

            assertTrue( "timestamp " + timestamp + " (" + output + ") is less than start " + start, start <= timestamp );
            assertTrue( "timestamp " + timestamp + " (" + output + ") is greater than end " + end, end >= timestamp );
        }
        catch ( ParseException e ) {
            fail( "Could not parse Now() output " + output + " with parseString " + parseString );
        }
    }

    @Test
    public void test1() throws Exception {
        _helper( new Now(), "yyyy-MM-dd HH:mm:ss", 1000L );
    }

    @Test
    public void test2() throws Exception {
        String formatString = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

        _helper( new Now( formatString ), formatString, 0L );
    }
}
