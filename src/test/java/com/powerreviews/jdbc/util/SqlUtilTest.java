package com.powerreviews.jdbc.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by dado on 4/7/16.
 */
public class SqlUtilTest {
    @Test
    public void testTokenizeStatement() {
        String statement1 = "select foo.field1, foo.field2 from boo.foo as foo " +
                "where foo.field1 = ? and foo.field3 in (?) " +
                "or (select roo.field1 from boo.roo as roo where roo.field2 <> ?)";
        String result1 = "select foo.field1, foo.field2 from boo.foo as foo " +
                "where foo.field1 = {0} and foo.field3 in ({1}) " +
                "or (select roo.field1 from boo.roo as roo where roo.field2 <> {2})";

        Assert.assertEquals(result1, SqlUtil.tokenizeStatement(statement1));

        String statement2 = "select * from boo.foo as foo where foo.type = 'bee'";
        String result2 = "select * from boo.foo as foo where foo.type = 'bee'";

        Assert.assertEquals(result2, SqlUtil.tokenizeStatement(statement2));
    }
}
