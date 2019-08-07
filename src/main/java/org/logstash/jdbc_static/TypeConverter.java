package org.logstash.jdbc_static;

import org.jruby.Ruby;
import org.jruby.RubyBignum;
import org.jruby.ext.bigdecimal.RubyBigDecimal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import static org.logstash.ext.JrubyTimestampExtLibrary.RubyTimestamp;

class TypeConverter {
    private static final Map<Integer, TypeConverter.Converter> CONVERTER_MAP = initConverters();

    private TypeConverter() {
    }

    static ConvertResult convertToRuby(final Ruby ruby, final String field, final ResultSetMetaData meta, final int index, final ResultSet rs) throws SQLException {
        final ConvertResult result = new TypeConvertResult(ruby.getNil());
        final int columnType = meta.getColumnType(index);
        final TypeConverter.Converter converter = CONVERTER_MAP.get(columnType);
        if (converter != null) {
            converter.convert(ruby, rs, index, field, result);
        } else {
            result.setMessage(field, meta.getColumnTypeName(index));
        }
        return result;
    }

    private static Map<Integer, TypeConverter.Converter> initConverters() {
        final Map<Integer, TypeConverter.Converter> converters = new HashMap<>(19);
        // I decided not to use lambdas here and used anonymous classes instead.
        final TypeConverter.Converter intconv = new TypeConverter.Converter() {
            @Override
            public void convert(final Ruby ruby, final ResultSet rs, final int index, final String field, final ConvertResult result) throws SQLException {
                final long x = rs.getLong(index);
                result.succeeded(field, x == 0L && rs.wasNull() ? ruby.getNil() : ruby.newFixnum(x));
            }
        };
        converters.put(Types.TINYINT, intconv);
        converters.put(Types.SMALLINT, intconv);
        converters.put(Types.INTEGER, intconv);

        final TypeConverter.Converter floatconv = new TypeConverter.Converter() {
            @Override
            public void convert(final Ruby ruby, final ResultSet rs, final int index, final String field, final ConvertResult result) throws SQLException {
                final double dbl = rs.getDouble(index);
                result.succeeded(field, dbl == 0.0d && rs.wasNull() ? ruby.getNil() : ruby.newFloat(dbl));
            }
        };

        converters.put(Types.REAL, floatconv);
        converters.put(Types.FLOAT, floatconv);
        converters.put(Types.DOUBLE, floatconv);

        converters.put(Types.BIGINT, new TypeConverter.Converter() {
            @Override
            public void convert(final Ruby ruby, final ResultSet rs, final int index, final String field, final ConvertResult result) throws SQLException {
                final String bigInt = rs.getString(index);
                result.succeeded(field, bigInt == null ? ruby.getNil() : RubyBignum.bignorm(ruby, new BigInteger(bigInt)));
            }
        });

        final TypeConverter.Converter decimalconv = new TypeConverter.Converter() {
            @Override
            public void convert(final Ruby ruby, final ResultSet rs, final int index, final String field, final ConvertResult result) throws SQLException {
                final BigDecimal bd = rs.getBigDecimal(index);
                result.succeeded(field, bd == null ? ruby.getNil() : new RubyBigDecimal(ruby, bd));
            }
        };
        converters.put(Types.NUMERIC, decimalconv);
        converters.put(Types.DECIMAL, decimalconv);

        converters.put(Types.DATE, new TypeConverter.Converter() {
            @Override
            public void convert(final Ruby ruby, final ResultSet rs, final int index, final String field, final ConvertResult result) throws SQLException {
                final Date date = rs.getDate(index);
                result.succeeded(field, date == null ? ruby.getNil() : RubyTimestamp.newRubyTimestamp(ruby, new org.logstash.Timestamp(date.getTime())));
            }
        });
        converters.put(Types.TIME, new TypeConverter.Converter() {
            @Override
            public void convert(final Ruby ruby, final ResultSet rs, final int index, final String field, final ConvertResult result) throws SQLException {
                final Time time = rs.getTime(index);
                result.succeeded(field, time == null ? ruby.getNil() : RubyTimestamp.newRubyTimestamp(ruby, new org.logstash.Timestamp(time.getTime())));
            }
        });
        converters.put(Types.TIMESTAMP, new TypeConverter.Converter() {
            @Override
            public void convert(final Ruby ruby, final ResultSet rs, final int index, final String field, final ConvertResult result) throws SQLException {
                final Timestamp ts = rs.getTimestamp(index);
                result.succeeded(field, ts == null ? ruby.getNil() : RubyTimestamp.newRubyTimestamp(ruby, new org.logstash.Timestamp(ts.getTime())));
            }
        });

        final TypeConverter.Converter boolconv = new TypeConverter.Converter() {
            @Override
            public void convert(final Ruby ruby, final ResultSet rs, final int index, final String field, final ConvertResult result) throws SQLException {
                result.succeeded(field, rs.getBoolean(index) ? ruby.getTrue() : ruby.getFalse());
            }
        };
        converters.put(Types.BIT, boolconv);
        converters.put(Types.BOOLEAN, boolconv);

        converters.put(Types.NULL, new TypeConverter.Converter() {
            @Override
            public void convert(final Ruby ruby, final ResultSet rs, final int index, final String field, final ConvertResult result) {
                result.succeeded(field, ruby.getNil());
            }
        });

        final TypeConverter.Converter charconv = new TypeConverter.Converter() {
            @Override
            public void convert(final Ruby ruby, final ResultSet rs, final int index, final String field, final ConvertResult result) throws SQLException {
                final String str = rs.getString(index);
                result.succeeded(field, str == null ? ruby.getNil() : ruby.newString(str));
            }
        };
        converters.put(Types.CHAR, charconv);
        converters.put(Types.VARCHAR, charconv);

        final TypeConverter.Converter ncharconv = new TypeConverter.Converter() {
            @Override
            public void convert(final Ruby ruby, final ResultSet rs, final int index, final String field, final ConvertResult result) throws SQLException {
                final String nstr = rs.getNString(index);
                result.succeeded(field, nstr == null ? ruby.getNil() : ruby.newString(nstr));
            }
        };
        converters.put(Types.NCHAR, ncharconv);
        converters.put(Types.NVARCHAR, ncharconv);

        return converters;
    }

    interface Converter {
        void convert(Ruby ruby, ResultSet rs, int index, String field, ConvertResult result) throws SQLException;
    }


}
