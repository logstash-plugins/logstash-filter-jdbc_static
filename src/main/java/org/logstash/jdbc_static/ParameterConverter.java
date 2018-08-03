package org.logstash.jdbc_static;

import org.jruby.RubyBignum;
import org.jruby.RubyFixnum;
import org.jruby.RubyNil;
import org.jruby.RubyNumeric;
import org.jruby.RubyString;
import org.jruby.ext.bigdecimal.RubyBigDecimal;
import org.jruby.runtime.builtin.IRubyObject;
import org.logstash.ext.JrubyTimestampExtLibrary;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

class ParameterConverter {
    private static final Map<String, ParameterConverter.Converter> CONVERTER_MAP = initConverters();

    private ParameterConverter() {
    }

    static void convertRubyForPreparedStatement(final int index, final PreparedStatement ps, final IRubyObject eventValue) throws SQLException {
        final ParameterConverter.Converter converter = CONVERTER_MAP.get(getType(eventValue));
        if (converter == null) {
            ps.setNull(index, Types.INTEGER);
        } else {
            converter.convert(index, ps, eventValue);
        }
    }

    private static Map<String, ParameterConverter.Converter> initConverters() {
        final Map<String, ParameterConverter.Converter> converters = new HashMap<>(19);
        // I decided not to use lambdas here and used anonymous classes instead.
        final ParameterConverter.Converter strconv = new ParameterConverter.Converter() {
            @Override
            public void convert(final int index, final PreparedStatement ps, final IRubyObject eventValue) throws SQLException {
                ps.setString(index, eventValue.toString());
            }
        };
        converters.put("String", strconv);
        converters.put("Symbol", strconv);

        converters.put("BigDecimal", new ParameterConverter.Converter() {
            @Override
            public void convert(final int index, final PreparedStatement ps, final IRubyObject eventValue) throws SQLException {
                ps.setBigDecimal(index, ((RubyBigDecimal) eventValue).getValue());
            }
        });

        converters.put("Fixnum", new ParameterConverter.Converter() {
            @Override
            public void convert(final int index, final PreparedStatement ps, final IRubyObject eventValue) throws SQLException {
                ps.setLong(index, ((RubyNumeric) eventValue).getLongValue());
            }
        });

        final ParameterConverter.Converter bignumconv = new ParameterConverter.Converter() {
            @Override
            public void convert(final int index, final PreparedStatement ps, final IRubyObject eventValue) throws SQLException {
                final BigInteger bigInt = ((RubyBignum) eventValue).getValue();
                if (bigInt.bitLength() <= 63) {
                    ps.setLong(index, bigInt.longValue());
                } else {
                    ps.setBigDecimal(index, new BigDecimal(bigInt));
                }
            }
        };
        converters.put("Bignum", bignumconv);

        final ParameterConverter.Converter floatconv = new ParameterConverter.Converter() {
            @Override
            public void convert(final int index, final PreparedStatement ps, final IRubyObject eventValue) throws SQLException {
                ps.setDouble(index, ((RubyNumeric) eventValue).getDoubleValue());
            }
        };
        converters.put("Float", floatconv);
        converters.put("Numeric", floatconv);

        converters.put("TrueClass", new ParameterConverter.Converter() {
            @Override
            public void convert(final int index, final PreparedStatement ps, final IRubyObject eventValue) throws SQLException {
                ps.setBoolean(index, true);
            }
        });
        converters.put("FalseClass", new ParameterConverter.Converter() {
            @Override
            public void convert(final int index, final PreparedStatement ps, final IRubyObject eventValue) throws SQLException {
                ps.setBoolean(index, false);
            }
        });

        final ParameterConverter.Converter tsconv = new ParameterConverter.Converter() {
            @Override
            public void convert(final int index, final PreparedStatement ps, final IRubyObject eventValue) throws SQLException {
                final long millis = ((JrubyTimestampExtLibrary.RubyTimestamp) eventValue).getTimestamp().getTime().getMillis();
                ps.setTimestamp(index, new Timestamp(millis));
            }
        };
        converters.put("Timestamp", tsconv);
        return converters;
    }

    private static String getType(final IRubyObject o) {
        if (o instanceof RubyString) {
            return "String";
        }
        if (o instanceof RubyFixnum) {
            return "Fixnum";
        }
        if (o instanceof RubyBigDecimal) {
            return "BigDecimal";
        }
        if (o instanceof JrubyTimestampExtLibrary.RubyTimestamp) {
            return "Timestamp";
        }
        if (o instanceof RubyNil) {
            return "RubyNil";
        }
        return o.getMetaClass().getRealClass().getSimpleName();
    }

    interface Converter {
        void convert(int index, PreparedStatement ps, IRubyObject eventValue) throws SQLException;
    }
}
