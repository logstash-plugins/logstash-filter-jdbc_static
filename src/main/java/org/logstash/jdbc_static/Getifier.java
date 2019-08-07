package org.logstash.jdbc_static;

import org.jruby.RubyArray;
import org.jruby.RubyHash;
import org.jruby.RubyString;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.logstash.ext.JrubyEventExtLibrary;
import org.logstash.ext.LookupFailures;

public class Getifier implements Fetchifier {
    private final RubyString reference;

    public Getifier(final RubyString reference) {
        this.reference = reference;
    }

    @Override

    public final IRubyObject fetch(final ThreadContext ctx, final JrubyEventExtLibrary.RubyEvent event, final LookupFailures lookupFailures) {
        final IRubyObject iRubyObject = event.ruby_get_field(ctx, reference);
        if (iRubyObject.isNil() || iRubyObject instanceof RubyHash || iRubyObject instanceof RubyArray) {
            lookupFailures.invalidParameterPush(reference);
            return reference;
        }
        return iRubyObject;
    }
}
