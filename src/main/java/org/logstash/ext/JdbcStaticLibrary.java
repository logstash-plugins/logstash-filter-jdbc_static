package org.logstash.ext;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
import org.jruby.runtime.load.Library;

public class JdbcStaticLibrary implements Library {

    @Override
    public final void load(final Ruby runtime, final boolean wrap) {
        final RubyModule logstashModule = runtime.getOrCreateModule("LogStash");
        final RubyModule filtersModule = runtime.defineModuleUnder("Filters", logstashModule);
        final RubyModule jdbcModule = runtime.defineModuleUnder("Jdbc", filtersModule);
        final RubyClass fetchClass = runtime.defineClassUnder("Fetcher", runtime.getObject(), RubyFetcher::new, jdbcModule);
        fetchClass.defineAnnotatedMethods(RubyFetcher.class);

        final RubyClass lookupFailuresClass = runtime.defineClassUnder("LookupFailures", runtime.getObject(), LookupFailures::new, jdbcModule);
        lookupFailuresClass.defineAnnotatedMethods(LookupFailures.class);
    }
}
