package org.logstash.jdbc_static;

import org.jruby.Ruby;
import org.jruby.RubyBignum;
import org.jruby.ext.bigdecimal.RubyBigDecimal;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.Library;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.logstash.ext.JrubyTimestampExtLibrary;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;


@RunWith(MockitoJUnitRunner.class)
public class ParameterConverterTest {
    private static final Ruby RUBY;

    static {
        RUBY = Ruby.newInstance();
        RubyBigDecimal.createBigDecimal(RUBY);
        final Library lib = new JrubyTimestampExtLibrary();
        try {
            lib.load(RUBY, true);
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    @Mock
    private PreparedStatement ps;

    private final int index = 1;

    @Test
    public void convertString() throws SQLException {
        ParameterConverter.convertRubyForPreparedStatement(index, ps, RUBY.newString("foo"));
        ParameterConverter.convertRubyForPreparedStatement(index, ps, RUBY.newSymbol("foo"));
        Mockito.verify(ps, Mockito.atLeast(2)).setString(index, "foo");
    }

    @Test
    public void convertBigDecimal() throws SQLException {
        ParameterConverter.convertRubyForPreparedStatement(index, ps, new RubyBigDecimal(RUBY, BigDecimal.valueOf(123456789.34d)));
        Mockito.verify(ps, Mockito.atLeast(1)).setBigDecimal(index, BigDecimal.valueOf(123456789.34d));
    }

    @Test
    public void convertFixnum() throws SQLException {
        ParameterConverter.convertRubyForPreparedStatement(index, ps, RUBY.newFixnum(123456789L));
        Mockito.verify(ps, Mockito.atLeast(1)).setLong(index, 123456789L);
    }

    @Test
    public void convertBignum() throws SQLException {
        ParameterConverter.convertRubyForPreparedStatement(index, ps, RubyBignum.bignorm(RUBY, new BigInteger("123456789")));
        Mockito.verify(ps, Mockito.atLeast(1)).setLong(index, 123456789L);
    }

    @Test
    public void convertBigBignum() throws SQLException {
        ParameterConverter.convertRubyForPreparedStatement(index, ps, RubyBignum.bignorm(RUBY, new BigInteger("123456789123456789123456789")));
        Mockito.verify(ps, Mockito.atLeast(1)).setBigDecimal(index, new BigDecimal(new BigInteger("123456789123456789123456789")));
    }

    @Test
    public void convertFloat() throws SQLException {
        ParameterConverter.convertRubyForPreparedStatement(index, ps, RUBY.newFloat(123456.789d));
        Mockito.verify(ps, Mockito.atLeast(1)).setDouble(index, 123456.789d);
    }

    @Test
    public void convertTrue() throws SQLException {
        ParameterConverter.convertRubyForPreparedStatement(index, ps, RUBY.getTrue());
        Mockito.verify(ps, Mockito.atLeast(1)).setBoolean(index, true);
    }

    @Test
    public void convertFalse() throws SQLException {
        ParameterConverter.convertRubyForPreparedStatement(index, ps, RUBY.getFalse());
        Mockito.verify(ps, Mockito.atLeast(1)).setBoolean(index, false);
    }

    @Test
    public void convertLogstashTimestamp() throws SQLException {
        final IRubyObject lrts = new JrubyTimestampExtLibrary.RubyTimestamp(RUBY, new org.logstash.Timestamp(9876123456789L));
        ParameterConverter.convertRubyForPreparedStatement(index, ps, lrts);
        Mockito.verify(ps, Mockito.atLeast(1)).setTimestamp(index, new Timestamp(9876123456789L));
    }

    @Test
    public void cannotConvertArray() throws SQLException {
        ParameterConverter.convertRubyForPreparedStatement(index, ps, RUBY.newArrayLight());
        Mockito.verify(ps, Mockito.atLeast(1)).setNull(index, Types.INTEGER);
    }
}