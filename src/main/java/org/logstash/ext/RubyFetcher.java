package org.logstash.ext;

import org.jruby.Ruby;
import org.jruby.RubyBoolean;
import org.jruby.RubyClass;
import org.jruby.RubyHash;
import org.jruby.RubyObject;
import org.jruby.RubyString;
import org.jruby.anno.JRubyMethod;
import org.jruby.exceptions.RaiseException;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.logstash.jdbc_static.FetchUpdate;
import org.logstash.jdbc_static.HikariCpDataSource;
import org.logstash.jdbc_static.Lookup;
import org.logstash.jdbc_static.PoolDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public final class RubyFetcher extends RubyObject {
    private static final long serialVersionUID = -2055943371574146412L;
    private RubyBoolean valid;
    private PoolDataSource pool;
    private Map<String, Lookup> lookups;
    private final RubyClass lookupFailuresRubyClass;

    public RubyFetcher(final Ruby runtime, final RubyClass metaClass) {
        super(runtime, metaClass);
        lookupFailuresRubyClass = runtime.getClassFromPath("LogStash::Filters::Jdbc").getClass("LookupFailures");
    }

    public RubyFetcher(final RubyClass metaClass) {
        super(metaClass);
        lookupFailuresRubyClass = Ruby.getGlobalRuntime().getClassFromPath("LogStash::Filters::Jdbc").getClass("LookupFailures");
    }

    @JRubyMethod(name = "valid?")
    public RubyBoolean getValid() {
        return valid;
    }

    @JRubyMethod
    public IRubyObject close(final ThreadContext ctx) {
        this.pool.close();
        return ctx.nil;
    }

    @JRubyMethod(name = "clone")
    public IRubyObject rubyClone(final ThreadContext ctx) {
        throw new RaiseException(ctx.runtime, ctx.runtime.getArgumentError(), "Please do not clone this class from ruby, create a new one instead", false);
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException("Java cloning of this JRuby Extension is not supported");
    }

    // def initialize(options) See the options passed to lookup.rb
    @JRubyMethod(name = "initialize", required = 1)
    public IRubyObject rubyInitialize(final ThreadContext ctx, final IRubyObject optionsHash) throws RaiseException {
        final Ruby ruby = ctx.runtime;
        this.valid = ruby.getFalse();
        final RubyHash options = (RubyHash) optionsHash;

        final RubyString rubyDriverJars = options.op_aref(ctx, ruby.newString("jdbc_driver_library")).asString();
        if (!(rubyDriverJars.isNil() || rubyDriverJars.isEmpty())) {
            final String[] libs = rubyDriverJars.toString().split(",");
            for (final String jar : libs) {
                ruby.getLoadService().require(jar);
            }
        }

        final IRubyObject rubyDriverName = options.fastARef(ruby.newString("jdbc_driver_class"));
        if (rubyDriverName == null) {
            throw new RaiseException(ruby, ruby.getArgumentError(), "Required option 'jdbc_driver_class' is missing", false);
        }
        final IRubyObject rubyUrl = options.fastARef(ruby.newString("jdbc_connection_string"));
        if (rubyUrl == null) {
            throw new RaiseException(ruby, ruby.getArgumentError(), "Required option 'jdbc_connection_string' is missing", false);
        }
        final RubyString username = options.op_aref(ctx, ruby.newString("username")).asString();
        final RubyString password = options.op_aref(ctx, ruby.newString("password")).asString();
        if (!(username.isNil() || username.isEmpty()) && !(password.isNil() || password.isEmpty())) {
            this.pool = new HikariCpDataSource(rubyDriverName.toString(), rubyUrl.toString(), username.toString(), password.toString());
        } else {
            this.pool = new HikariCpDataSource(rubyDriverName.toString(), rubyUrl.toString(), null, null);
        }
        try {
            final Connection conn = pool.getConnection(); //  SELECT 1 FROM SYS.SYSSCHEMAS FETCH FIRST 1 ROWS ONLY
            if (conn == null || !conn.isValid(1)) {
                final String msg = "Could not connect to the internal database, check the connection string";
                throw new RaiseException(ruby, ruby.getArgumentError(), msg, true);
            }
            conn.close();
        } catch (final SQLException e) {
            throw new RaiseException(ruby, ruby.getArgumentError(), e.getMessage(), true);
        }
        this.lookups = new HashMap<>(32);
        this.valid = ruby.getTrue();
        return ctx.nil;
    }

    // def add_lookup(options)
    @JRubyMethod(name = "add_lookup", required = 1)
    public IRubyObject addLookup(final ThreadContext ctx, final IRubyObject optionsHash) {
        final Ruby ruby = ctx.runtime;
        final RubyHash options = (RubyHash) optionsHash;

        final IRubyObject rubyId = options.fastARef(ruby.newString("id"));
        if (rubyId == null) {
            throw new RaiseException(ruby, ruby.getArgumentError(), "Required option 'id' is missing", false);
        }

        final IRubyObject rubyQuery = options.fastARef(ruby.newString("query"));
        if (rubyQuery == null) {
            throw new RaiseException(ruby, ruby.getArgumentError(), "Required option 'query' is missing", false);
        }

        final IRubyObject rubyParameters = options.fastARef(ruby.newString("parameters"));
        if (rubyParameters == null) {
            throw new RaiseException(ruby, ruby.getArgumentError(), "Required option 'parameters' is missing", false);
        }

        final IRubyObject rubyTarget = options.fastARef(ruby.newString("target"));
        if (rubyTarget == null) {
            throw new RaiseException(ruby, ruby.getArgumentError(), "Required option 'target' is missing", false);
        }
        final String id = rubyId.toString();
        this.lookups.put(id, new FetchUpdate(id, (RubyString) rubyQuery, (RubyHash) rubyParameters, (RubyString) rubyTarget));

        return ctx.nil;
    }

    // def fetch_and_update(id, event) // returns a LookupFailures
    @JRubyMethod(name = "fetch_and_update", required = 2)
    public IRubyObject fetchAndUpdate(final ThreadContext ctx, final IRubyObject id, final IRubyObject event) {
        final String key = id.toString();
        final LookupFailures lookupFailures = new LookupFailures(ctx.runtime, lookupFailuresRubyClass);
        final Lookup lookup = lookups.get(key);
        if (lookup == null) {
            lookupFailures.lookupIdIsInvalid();
        } else {
            lookup.fetchAndUpdate(ctx, pool, event, lookupFailures);
        }
        return lookupFailures;
    }
}
