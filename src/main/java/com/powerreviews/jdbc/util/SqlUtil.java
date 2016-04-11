package com.powerreviews.jdbc.util;

/**
 * Created by dado on 4/7/16.
 */
public class SqlUtil {
    /**
     * Given an SQL statement containing "?" placeholders, this function replaces question marks
     * with a placeholder of type "{POS}" where "POS" is the position of the question mark starting
     * from 0. I.e. the first question mark will be replaced with "{0}", the second with "{1}", etc.
     * NOTICE: this method does NOT work if question marks are used as literals in the query.
     * @param statement The query
     * @return The tokenized query
     */
    public static String tokenizeStatement(String statement) {
        String modifiedSql = statement;
        int variableCount = 0;
        while(modifiedSql.contains("?")) {
            modifiedSql = modifiedSql.replaceFirst("\\?", "{" + variableCount + "}");
            variableCount++;
        }
        return modifiedSql;
    }
}
