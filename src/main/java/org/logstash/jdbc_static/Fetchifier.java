package org.logstash.jdbc_static;

import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.logstash.ext.JrubyEventExtLibrary;
import org.logstash.ext.LookupFailures;

interface Fetchifier {
    IRubyObject fetch(ThreadContext ctx, JrubyEventExtLibrary.RubyEvent event, LookupFailures lookupFailures);
}
