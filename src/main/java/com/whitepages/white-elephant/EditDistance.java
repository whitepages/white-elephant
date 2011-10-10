package com.whitepages.pigenhancements;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * Calculates the Levenshtein distance between the two input strings.
 * Example:
 *      register pigudfs.jar;
 *      define ED com.whitepages.pigenhancements.EditDistance();
 *      A = load 'mydata' as string1, string2;
 *      B = foreach A generate ED(string1, string2);
 *      dump B;
 */

public class EditDistance extends EvalFunc<Integer>
{
    /**
     * @return the Edit distance between two strings
     */
    public Integer exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2)
            return null;

        try {
            String s1 = (String)input.get(0);
            String s2 = (String)input.get(1);
            return levenshteinDistance(s1, s2);
        } catch(Exception e){
            log.warn("Failed to process input; error - " + e.getMessage());
            return null;
        }
    }


    /**
     * Returns the Levenshtein distance between the two strings
     * Adapted from http://en.wikipedia.org/wiki/Levenshtein_distance 
     */
    // TODO  Reconcile this method with the similar method in ClassifySearchSession
    private int levenshteinDistance(String s, String t) {
      // for all i and j, d[i][j] will hold the Levenshtein distance between
      // the first i characters of s and the first j characters of t;
      // note that d has (m+1)x(n+1) values
      int m = s.length();
      int n = t.length();


      int[][] d = new int[m+1][n+1];

      // the distance of any first string to an empty second string
      for (int i = 0; i <= m; i++)
        d[i][0] = i;

      // the distance of any second string to an empty first string
      for (int j = 0; j <= n; j++)
        d[0][j] = j;

      for (int j = 1; j <= n; j++) {
        for (int i = 1; i <= m; i++) {
          if ( s.charAt(i-1) == t.charAt(j-1) )
            d[i][j] = d[i-1][j-1];       // no operation required
          else {
            int deletion = d[i-1][j] + 1;
            int insertion = d[i][j-1] + 1;
            int substitution = d[i-1][j-1] + 1;
            d[i][j] = Math.min(deletion, Math.min(insertion, substitution));
          }
        }
      }

      return d[m][n];
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.INTEGER));
    }

}
