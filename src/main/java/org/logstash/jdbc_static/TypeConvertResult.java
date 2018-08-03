package org.logstash.jdbc_static;

import org.jruby.runtime.builtin.IRubyObject;

public final class TypeConvertResult implements ConvertResult {
    private static final String MSG = "Could not convert SQL Type into suitable Ruby type to store in the event, column name is '%s', SQL type is '%s'";
    private boolean failure;
    private String field;
    private IRubyObject value;
    private String message;

    public TypeConvertResult(final IRubyObject value) {
        this.failure = true;
        this.field = "";
        this.value = value;
        this.message = "";
    }

    @Override
    public void succeeded(final String field, final IRubyObject value) {
        this.failure = false;
        this.field = field;
        this.value = value;
    }

    @Override
    public boolean isFailure() {
        return failure;
    }

    @Override
    public String getField() {
        return field;
    }

    @Override
    public IRubyObject getValue() {
        return value;
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public void setMessage(final String name, final String type) {
        this.message = String.format(MSG, name, type);
    }
}
