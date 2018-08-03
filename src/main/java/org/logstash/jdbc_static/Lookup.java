package org.logstash.jdbc_static;

import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.logstash.ext.LookupFailures;

public interface Lookup {
    void fetchAndUpdate(ThreadContext ctx, PoolDataSource pool, IRubyObject event, LookupFailures lookupFailures);
}
