package org.logstash.ext;

import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyClass;
import org.jruby.RubyObject;
import org.jruby.RubyString;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

import java.util.HashSet;
import java.util.Set;

public final class LookupFailures extends RubyObject {
    private RubyArray invalidParameters;
    private Set<String> invalidColumns = new HashSet<>(256);
    private boolean lookupByIdInvalid = false;

    public LookupFailures(final Ruby runtime, final RubyClass metaClass) {
        super(runtime, metaClass);
        javaInit(runtime);
    }

    public LookupFailures(final RubyClass metaClass) {
        super(metaClass);
        javaInit(metaClass.getRuntime());
    }

    private void javaInit(final Ruby ruby) {
        this.invalidParameters = ruby.newArrayLight();
    }

    @JRubyMethod(name = "initialize")
    public IRubyObject rubyInitialize(final ThreadContext ctx) {
        javaInit(ctx.runtime);
        return ctx.nil;
    }

    @JRubyMethod(name = "invalid_parameters")
    public IRubyObject rubyInvalidParameters(final ThreadContext ctx) {
        return invalidParameters;
    }

    @JRubyMethod(name = "invalid_columns")
    public IRubyObject rubyInvalidColumns(final ThreadContext ctx) {
        final RubyArray output = RubyArray.newArray(ctx.runtime, invalidColumns.size());
        for (final String message : invalidColumns) {
            output.push(RubyString.newString(ctx.runtime, message));
        }
        return output;
    }

    @JRubyMethod(name = "invalid_id_for_lookup?")
    public IRubyObject rubyInvalidIdForLookupP(final ThreadContext ctx) {
        return ctx.runtime.newBoolean(this.lookupByIdInvalid);
    }

    @JRubyMethod(name = "any_invalid_columns?")
    public IRubyObject anyInvalidColumnsP(final ThreadContext ctx) {
        return ctx.runtime.newBoolean(!this.invalidColumns.isEmpty());
    }

    @JRubyMethod(name = "any_invalid_parameters?")
    public IRubyObject anyInvalidParametersP(final ThreadContext ctx) {
        return ctx.runtime.newBoolean(parametersAreInvalid());
    }

    public void invalidParameterPush(final IRubyObject invalidParameter) {
        invalidParameters.push(invalidParameter);
    }

    public void invalidColumnPush(final String invalidColumnMessage) {
        invalidColumns.add(invalidColumnMessage);
    }

    public void lookupIdIsInvalid() {
        this.lookupByIdInvalid = true;
    }

    public boolean parametersAreValid() {
        return invalidParameters.isEmpty();
    }

    public boolean parametersAreInvalid() {
        return !invalidParameters.isEmpty();
    }
}
