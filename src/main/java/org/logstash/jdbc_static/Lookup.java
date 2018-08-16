package org.logstash.jdbc_static;

import org.jruby.runtime.ThreadContext;
import org.logstash.ext.JrubyEventExtLibrary;
import org.logstash.ext.LookupFailures;

public interface Lookup {
    void fetchAndUpdate(ThreadContext ctx, PoolDataSource pool, JrubyEventExtLibrary.RubyEvent event, LookupFailures lookupFailures);
}
