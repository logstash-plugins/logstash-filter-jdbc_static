package org.logstash.jdbc_static;

import org.jruby.RubyString;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.logstash.ext.JrubyEventExtLibrary;
import org.logstash.ext.LookupFailures;

import java.io.IOException;

public class Sprintifier implements Fetchifier {
    private final RubyString reference;

    public Sprintifier(final RubyString reference) {
        this.reference = reference;
    }

    @Override
    public final IRubyObject fetch(final ThreadContext ctx, final JrubyEventExtLibrary.RubyEvent event, final LookupFailures lookupFailures) {
        final RubyString string;
        try {
            string = RubyString.newString(ctx.runtime, event.getEvent().sprintf(reference.toString()));
            if (reference.eql(string)) {
                lookupFailures.invalidParameterPush(reference);
                return reference;
            }
        } catch (IOException e) {
            lookupFailures.invalidParameterPush(reference);
            return reference;
        }
        return string;
    }
}
