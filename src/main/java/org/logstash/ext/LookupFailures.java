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
    private Set<String> allColumns = new HashSet<>(256);
    private boolean doColumnCheck = false;
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

    @JRubyMethod
    public IRubyObject clear(final ThreadContext ctx) {
        this.invalidParameters.clear();
        this.invalidColumns.clear();
        this.doColumnCheck = false;
        return ctx.nil;
    }

    @JRubyMethod(name = "check_columns")
    public IRubyObject setDoColumnCheck(ThreadContext ctx) {
        this.doColumnCheck = true;
        return this;
    }

    @JRubyMethod(name = "checking_columns?")
    public IRubyObject checkingColumnsP(ThreadContext ctx) {
        return this.doColumnCheck ? ctx.tru : ctx.fals;
    }

    @JRubyMethod(name = "invalid_parameters")
    public IRubyObject rubyInvalidParameters(final ThreadContext ctx) {
        return invalidParameters;
    }

    @JRubyMethod(name = "all_columns")
    public IRubyObject rubyAllColumns(final ThreadContext ctx) {
        final RubyArray output = RubyArray.newArray(ctx.runtime, allColumns.size());
        for (final String column : allColumns) {
            output.append(RubyString.newString(ctx.runtime, column));
        }
        return output;
    }

    @JRubyMethod(name = "invalid_columns")
    public IRubyObject rubyInvalidColumns(final ThreadContext ctx) {
        final RubyArray output = RubyArray.newArray(ctx.runtime, invalidColumns.size());
        for (final String message : invalidColumns) {
            output.append(RubyString.newString(ctx.runtime, message));
        }
        return output;
    }

    @JRubyMethod(name= "successful?")
    public IRubyObject rubySuccessfulP(final ThreadContext ctx) {
        return this.lookupByIdInvalid || this.invalidColumns.size() > 0 || parametersAreInvalid() ? ctx.fals : ctx.tru;
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

    public Set<String> getInvalidColumns() {
        return invalidColumns;
    }

    public boolean isCheckingColumns() {
        return doColumnCheck;
    }

    public void invalidParameterPush(final IRubyObject invalidParameter) {
        invalidParameters.append(invalidParameter);
    }

    public void columnPush(final String validColumn) {
        if (doColumnCheck) {
            allColumns.add(validColumn);
        }
    }

    public void invalidColumnPush(final String invalidColumnMessage) {
        if (doColumnCheck) {
            invalidColumns.add(invalidColumnMessage);
        }
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
