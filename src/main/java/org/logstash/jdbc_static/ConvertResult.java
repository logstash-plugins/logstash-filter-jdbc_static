package org.logstash.jdbc_static;

import org.jruby.runtime.builtin.IRubyObject;

public interface ConvertResult {
    void succeeded(String field, IRubyObject value);

    boolean isFailure();

    String getField();

    IRubyObject getValue();

    String getMessage();

    void setMessage(String name, String type);
}
