package org.logstash.jdbc_static;

import org.jruby.Ruby;
import org.jruby.RubyBignum;
import org.jruby.ext.bigdecimal.RubyBigDecimal;
import org.jruby.runtime.builtin.IRubyObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import static org.logstash.ext.JrubyTimestampExtLibrary.RubyTimestamp;
import static org.logstash.jdbc_static.TestCommon.*;

@RunWith(MockitoJUnitRunner.class)
public class ParameterConverterTest {
    private Ruby ruby = Ruby.getGlobalRuntime();

    @Before
    public void setUp() throws Exception {
        loadRubyBigDecimal(ruby);
        tryLoadRubyTimestampLibrary(ruby);
    }

    @Mock
    private PreparedStatement ps;

    private final int index = 1;

    @Test
    public void convertString() throws SQLException {
        ParameterConverter.convertRubyForPreparedStatement(index, ps, ruby.newString("foo"));
        ParameterConverter.convertRubyForPreparedStatement(index, ps, ruby.newSymbol("foo"));
        Mockito.verify(ps, Mockito.atLeast(2)).setString(index, "foo");
    }

    @Test
    public void convertBigDecimal() throws SQLException {
        ParameterConverter.convertRubyForPreparedStatement(index, ps, new RubyBigDecimal(ruby, BigDecimal.valueOf(123456789.34d)));
        Mockito.verify(ps, Mockito.atLeast(1)).setBigDecimal(index, BigDecimal.valueOf(123456789.34d));
    }

    @Test
    public void convertFixnum() throws SQLException {
        ParameterConverter.convertRubyForPreparedStatement(index, ps, ruby.newFixnum(123456789L));
        Mockito.verify(ps, Mockito.atLeast(1)).setLong(index, 123456789L);
    }

    @Test
    public void convertBignum() throws SQLException {
        ParameterConverter.convertRubyForPreparedStatement(index, ps, RubyBignum.bignorm(ruby, new BigInteger("123456789")));
        Mockito.verify(ps, Mockito.atLeast(1)).setLong(index, 123456789L);
    }

    @Test
    public void convertBigBignum() throws SQLException {
        ParameterConverter.convertRubyForPreparedStatement(index, ps, RubyBignum.bignorm(ruby, new BigInteger("123456789123456789123456789")));
        Mockito.verify(ps, Mockito.atLeast(1)).setBigDecimal(index, new BigDecimal(new BigInteger("123456789123456789123456789")));
    }

    @Test
    public void convertFloat() throws SQLException {
        ParameterConverter.convertRubyForPreparedStatement(index, ps, ruby.newFloat(123456.789d));
        Mockito.verify(ps, Mockito.atLeast(1)).setDouble(index, 123456.789d);
    }

    @Test
    public void convertTrue() throws SQLException {
        ParameterConverter.convertRubyForPreparedStatement(index, ps, ruby.getTrue());
        Mockito.verify(ps, Mockito.atLeast(1)).setBoolean(index, true);
    }

    @Test
    public void convertFalse() throws SQLException {
        ParameterConverter.convertRubyForPreparedStatement(index, ps, ruby.getFalse());
        Mockito.verify(ps, Mockito.atLeast(1)).setBoolean(index, false);
    }

    @Test
    public void convertLogstashTimestamp() throws SQLException {
        final IRubyObject lrts = RubyTimestamp.newRubyTimestamp(ruby, new org.logstash.Timestamp(9876123456789L));
        ParameterConverter.convertRubyForPreparedStatement(index, ps, lrts);
        Mockito.verify(ps, Mockito.atLeast(1)).setTimestamp(index, new Timestamp(9876123456789L));
    }

    @Test
    public void cannotConvertArray() throws SQLException {
        ParameterConverter.convertRubyForPreparedStatement(index, ps, ruby.newArrayLight());
        Mockito.verify(ps, Mockito.atLeast(1)).setNull(index, Types.INTEGER);
    }
}