package org.logstash.jdbc_static;

import org.assertj.core.api.Assertions;
import org.jruby.Ruby;
import org.jruby.RubyBignum;
import org.jruby.RubyInteger;
import org.jruby.RubyObject;
import org.jruby.ext.bigdecimal.RubyBigDecimal;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.Library;
import org.junit.Test;
import org.logstash.ext.JrubyTimestampExtLibrary;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class GetifierTest {

    private static final Ruby RUBY = Ruby.newInstance();

    static {
        RubyBigDecimal.createBigDecimal(RUBY);
        final Library lib = new JrubyTimestampExtLibrary();
        try {
            lib.load(RUBY, true);
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    private String getType(final IRubyObject o) {
        return o.getMetaClass().getRealClass().getSimpleName();
    }

    @Test
    public void name() {
        // converters.put(RubyNil.class, IDENTITY);
        Assertions.assertThat(getType((RubyObject)RUBY.getNil())).isEqualTo("NilClass");
        // converters.put(RubyString.class, IDENTITY);
        Assertions.assertThat(getType(RUBY.newString("foo"))).isEqualTo("String");
        // converters.put(RubySymbol.class, IDENTITY);
        final RubyObject sym = RUBY.newSymbol("foo");
        Assertions.assertThat(getType(sym)).isEqualTo("Symbol");
        Assertions.assertThat(sym.toString()).isEqualTo("foo");
        // converters.put(RubyBignum.class, IDENTITY);
        Assertions.assertThat(getType(new RubyBignum(RUBY, BigInteger.valueOf(1L)))).isEqualTo("Bignum");
        // converters.put(RubyBigDecimal.class, IDENTITY);
        Assertions.assertThat(getType(new RubyBigDecimal(RUBY, BigDecimal.valueOf(1L)))).isEqualTo("BigDecimal");
        // converters.put(RubyFloat.class, IDENTITY);
        Assertions.assertThat(getType(RUBY.newFloat(2.0d))).isEqualTo("Float");
        // converters.put(RubyFixnum.class, IDENTITY);
        final RubyInteger rInt = RUBY.newFixnum(1234567L);
        Assertions.assertThat(getType(rInt)).isEqualTo("Fixnum");
        final BigInteger bigInt = rInt.convertToInteger().getBigIntegerValue();
        Assertions.assertThat(bigInt.bitLength()).isLessThanOrEqualTo(63);
        Assertions.assertThat(getType(RUBY.newNumeric())).isEqualTo("Numeric");
        // converters.put(RubyBoolean.class, IDENTITY);
        Assertions.assertThat(getType(RUBY.getTrue())).isEqualTo("TrueClass");
        Assertions.assertThat(getType(RUBY.getFalse())).isEqualTo("FalseClass");
        Assertions.assertThat(getType(RUBY.newBoolean(true))).isEqualTo("TrueClass");
        Assertions.assertThat(getType(RUBY.newBoolean(false))).isEqualTo("FalseClass");
        // converters.put(JrubyTimestampExtLibrary.RubyTimestamp.class, IDENTITY);
        Assertions.assertThat(getType(JrubyTimestampExtLibrary.RubyTimestamp.newRubyTimestamp(RUBY, 100L))).isEqualTo("Timestamp");
    }
}