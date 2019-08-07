package org.logstash.jdbc_static;

import org.jruby.Ruby;
import org.jruby.RubyBignum;
import org.jruby.ext.bigdecimal.RubyBigDecimal;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.logstash.Timestamp;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;
import static org.logstash.ext.JrubyTimestampExtLibrary.RubyTimestamp;
import static org.logstash.jdbc_static.TestCommon.loadRubyBigDecimal;
import static org.logstash.jdbc_static.TestCommon.tryLoadRubyTimestampLibrary;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TypeConverterTest {
    private Ruby ruby = Ruby.getGlobalRuntime();

    @Mock
    private ResultSetMetaData meta;

    @Mock
    private ResultSet rs;

    private final int index = 1;
    private final String field = "field1";

    @Before
    public void setUp() throws Exception {
        loadRubyBigDecimal(ruby);
        tryLoadRubyTimestampLibrary(ruby);
    }

    @Test
    public void convertIntToRuby() throws SQLException {
        when(meta.getColumnType(index))
                .thenReturn(Types.TINYINT, Types.SMALLINT, Types.INTEGER);
        when(rs.getLong(index)).thenReturn(42L);
        for (int i = 0; i < 3; i++) {
            final ConvertResult result = TypeConverter.convertToRuby(ruby, field, meta, index, rs);
            assertThat(result.getMessage()).isEmpty();
            assertThat(result.getField()).isEqualTo(field);
            assertThat(result.getValue()).isEqualTo(ruby.newFixnum(42L));
        }
    }

    @Test
    public void convertFloatToRuby() throws SQLException {
        when(meta.getColumnType(index)).thenReturn(Types.REAL, Types.FLOAT, Types.DOUBLE);
        when(rs.getDouble(index)).thenReturn(42.0d);
        for (int i = 0; i < 3; i++) {
            final ConvertResult result = TypeConverter.convertToRuby(ruby, field, meta, index, rs);
            assertThat(result.getMessage()).isEmpty();
            assertThat(result.getField()).isEqualTo(field);
            assertThat(result.getValue()).isEqualTo(ruby.newFloat(42.0d));
        }
    }

    @Test
    public void convertVarCharToRuby() throws SQLException {
        when(meta.getColumnType(index)).thenReturn(Types.CHAR, Types.VARCHAR);
        when(rs.getString(index)).thenReturn("foo");
        for (int i = 0; i < 2; i++) {
            final ConvertResult result = TypeConverter.convertToRuby(ruby, field, meta, index, rs);
            assertThat(result.getMessage()).isEmpty();
            assertThat(result.getField()).isEqualTo(field);
            assertThat(result.getValue()).isEqualTo(ruby.newString("foo"));
        }
    }

    @Test
    public void convertNVarcharToRuby() throws SQLException {
        when(meta.getColumnType(index)).thenReturn(Types.NCHAR, Types.NVARCHAR);
        when(rs.getNString(index)).thenReturn("foo", "bar");
        final String[] actuals = {"foo", "bar"};
        for (int i = 0; i < 2; i++) {
            final ConvertResult result = TypeConverter.convertToRuby(ruby, field, meta, index, rs);
            assertThat(result.getMessage()).isEmpty();
            assertThat(field).isEqualTo(result.getField());
            assertThat(ruby.newString(actuals[i])).isEqualTo(result.getValue());
        }
    }

    @Test
    public void convertBigIntToRuby() throws SQLException {
        when(meta.getColumnType(index)).thenReturn(Types.BIGINT);
        when(rs.getString(index)).thenReturn("123456789");
        final ConvertResult result = TypeConverter.convertToRuby(ruby, field, meta, index, rs);
        assertThat(result.getMessage()).isEmpty();
        assertThat(result.getField()).isEqualTo(field);
        assertThat(result.getValue()).isEqualTo(RubyBignum.bignorm(ruby, new BigInteger("123456789")));
    }

    @Test
    public void convertDecimalToRuby() throws SQLException {
        when(meta.getColumnType(index)).thenReturn(Types.NUMERIC, Types.DECIMAL);
        when(rs.getBigDecimal(index)).thenReturn(BigDecimal.valueOf(123456789.34d));
        for (int i = 0; i < 2; i++) {
            final ConvertResult result = TypeConverter.convertToRuby(ruby, field, meta, index, rs);
            assertThat(result.getMessage()).isEmpty();
            assertThat(result.getField()).isEqualTo(field);
            assertThat(result.getValue()).isEqualTo(new RubyBigDecimal(ruby, new BigDecimal("123456789.34")));
        }
    }

    @Test
    public void convertDateToRuby() throws SQLException {
        when(meta.getColumnType(index)).thenReturn(Types.DATE);
        when(rs.getDate(index)).thenReturn(new Date(123456789L));
        final ConvertResult result = TypeConverter.convertToRuby(ruby, field, meta, index, rs);
        assertThat(result.getMessage()).isEmpty();
        assertThat(result.getField()).isEqualTo(field);
        final Timestamp expected = new Timestamp(123456789L);
        final Timestamp actual = ((RubyTimestamp) result.getValue()).getTimestamp();
        assertThat(expected.getTime()).isEqualTo(actual.getTime());
    }

    @Test
    public void convertTimeToRuby() throws SQLException {
        when(meta.getColumnType(index)).thenReturn(Types.TIME);
        when(rs.getTime(index)).thenReturn(new Time(123456789L));
        final ConvertResult result = TypeConverter.convertToRuby(ruby, field, meta, index, rs);
        assertThat(result.getMessage()).isEmpty();
        assertThat(result.getField()).isEqualTo(field);
        final Timestamp expected = new Timestamp(123456789L);
        final Timestamp actual = ((RubyTimestamp) result.getValue()).getTimestamp();
        assertThat(expected.getTime()).isEqualTo(actual.getTime());
    }

    @Test
    public void convertTimestampToRuby() throws SQLException {
        when(meta.getColumnType(index)).thenReturn(Types.TIMESTAMP);
        when(rs.getTimestamp(index)).thenReturn(new java.sql.Timestamp(123456789L));
        final ConvertResult result = TypeConverter.convertToRuby(ruby, field, meta, index, rs);
        assertThat(result.getMessage()).isEmpty();
        assertThat(result.getField()).isEqualTo(field);
        final Timestamp expected = new Timestamp(123456789L);
        final Timestamp actual = ((RubyTimestamp) result.getValue()).getTimestamp();
        assertThat(expected.getTime()).isEqualTo(actual.getTime());
    }

    @Test
    public void convertBooleanToRuby() throws SQLException {
        when(meta.getColumnType(index)).thenReturn(Types.BIT, Types.BOOLEAN);
        when(rs.getBoolean(index)).thenReturn(true, false);
        final ConvertResult result1;
        final ConvertResult result2;
        result1 = TypeConverter.convertToRuby(ruby, field, meta, index, rs);
        result2 = TypeConverter.convertToRuby(ruby, field, meta, index, rs);
        assertThat(result1.getMessage()).isEmpty();
        assertThat(result2.getMessage()).isEmpty();
        assertThat(field).isEqualTo(result1.getField());
        assertThat(field).isEqualTo(result2.getField());
        assertThat(ruby.getTrue()).isEqualTo(result1.getValue());
        assertThat(ruby.getFalse()).isEqualTo(result2.getValue());
    }

    @Test
    public void convertNullToRuby() throws SQLException {
        when(meta.getColumnType(index)).thenReturn(Types.BIGINT);
        when(rs.getString(index)).thenReturn(null);
        final ConvertResult result = TypeConverter.convertToRuby(ruby, field, meta, index, rs);
        assertThat(result.getMessage()).isEmpty();
        assertThat(field).isEqualTo(result.getField());
        assertThat(ruby.getNil()).isEqualTo(result.getValue());
    }

    @Test
    public void convertUnsupportedSqlTypeToRuby() throws SQLException {
        when(meta.getColumnTypeName(index)).thenReturn("Oops");
        when(meta.getColumnType(index)).thenReturn(Types.ARRAY);
        final ConvertResult result = TypeConverter.convertToRuby(ruby, field, meta, index, rs);
        assertThat(result.isFailure()).isTrue();
        assertThat("Could not convert SQL Type into suitable Ruby type to store in the event, column name is 'field1', SQL type is 'Oops'")
                .isEqualTo(result.getMessage());

    }
}