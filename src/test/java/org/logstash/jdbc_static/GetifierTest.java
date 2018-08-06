package org.logstash.jdbc_static;

import org.assertj.core.api.Assertions;
import org.jruby.Ruby;
import org.jruby.RubyBignum;
import org.jruby.RubyInteger;
import org.jruby.RubyObject;
import org.jruby.ext.bigdecimal.RubyBigDecimal;
import org.jruby.runtime.builtin.IRubyObject;
import org.junit.Before;
import org.junit.Test;
import org.logstash.ext.JrubyTimestampExtLibrary;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.logstash.jdbc_static.TestCommon.isLogstashTimestampLoaded;
import static org.logstash.jdbc_static.TestCommon.loadRubyBigDecimal;
import static org.logstash.jdbc_static.TestCommon.loadViaLibraryLoad;
import static org.logstash.jdbc_static.TestCommon.loadViaRubyUtil;

public class GetifierTest {

    private Ruby ruby = Ruby.newInstance();

    @Before
    public void setUp() throws Exception {
        ruby = Ruby.newInstance();
        loadRubyBigDecimal(ruby);
        boolean progress = isLogstashTimestampLoaded(ruby);
        if (!progress) {
            progress = loadViaRubyUtil(ruby);
        }
        if (!progress) {
            loadViaLibraryLoad(ruby);
        }
    }

    private String getType(final IRubyObject o) {
        return o.getMetaClass().getRealClass().getSimpleName();
    }

    @Test
    public void name() {
        // converters.put(RubyNil.class, IDENTITY);
        Assertions.assertThat(getType((RubyObject) ruby.getNil())).isEqualTo("NilClass");
        // converters.put(RubyString.class, IDENTITY);
        Assertions.assertThat(getType(ruby.newString("foo"))).isEqualTo("String");
        // converters.put(RubySymbol.class, IDENTITY);
        final RubyObject sym = ruby.newSymbol("foo");
        Assertions.assertThat(getType(sym)).isEqualTo("Symbol");
        Assertions.assertThat(sym.toString()).isEqualTo("foo");
        // converters.put(RubyBignum.class, IDENTITY);
        Assertions.assertThat(getType(new RubyBignum(ruby, BigInteger.valueOf(1L)))).isEqualTo("Bignum");
        // converters.put(RubyBigDecimal.class, IDENTITY);
        Assertions.assertThat(getType(new RubyBigDecimal(ruby, BigDecimal.valueOf(1L)))).isEqualTo("BigDecimal");
        // converters.put(RubyFloat.class, IDENTITY);
        Assertions.assertThat(getType(ruby.newFloat(2.0d))).isEqualTo("Float");
        // converters.put(RubyFixnum.class, IDENTITY);
        final RubyInteger rInt = ruby.newFixnum(1234567L);
        Assertions.assertThat(getType(rInt)).isEqualTo("Fixnum");
        final BigInteger bigInt = rInt.convertToInteger().getBigIntegerValue();
        Assertions.assertThat(bigInt.bitLength()).isLessThanOrEqualTo(63);
        Assertions.assertThat(getType(ruby.newNumeric())).isEqualTo("Numeric");
        // converters.put(RubyBoolean.class, IDENTITY);
        Assertions.assertThat(getType(ruby.getTrue())).isEqualTo("TrueClass");
        Assertions.assertThat(getType(ruby.getFalse())).isEqualTo("FalseClass");
        Assertions.assertThat(getType(ruby.newBoolean(true))).isEqualTo("TrueClass");
        Assertions.assertThat(getType(ruby.newBoolean(false))).isEqualTo("FalseClass");
        // converters.put(JrubyTimestampExtLibrary.RubyTimestamp.class, IDENTITY);
        Assertions.assertThat(getType(JrubyTimestampExtLibrary.RubyTimestamp.newRubyTimestamp(ruby, new org.logstash.Timestamp(9876543212345L)))).isEqualTo("Timestamp");
    }
}