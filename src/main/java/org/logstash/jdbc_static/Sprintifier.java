package org.logstash.jdbc_static;

import org.jruby.RubyString;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.logstash.ext.JrubyEventExtLibrary;
import org.logstash.ext.LookupFailures;

public class Sprintifier implements Fetchifier {
    private final RubyString reference;

    public Sprintifier(final RubyString reference) {
        this.reference = reference;
    }

    @Override
    public final IRubyObject fetch(final ThreadContext ctx, final JrubyEventExtLibrary.RubyEvent event, final LookupFailures lookupFailures) {
        final RubyString string = (RubyString) event.ruby_sprintf(ctx, reference);
        if (reference.eql(string)) {
            lookupFailures.invalidParameterPush(reference);
            return reference;
        }
        return string;
    }
}
