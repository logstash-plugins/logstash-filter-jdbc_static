package org.logstash.jdbc_static;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyHash;
import org.jruby.RubyString;
import org.jruby.exceptions.RaiseException;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.logstash.ext.JrubyEventExtLibrary;
import org.logstash.ext.LookupFailures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class FetchUpdate implements Lookup {
    private final String statement;
    private final String lookup_id;
    private final Map<String, Fetchifier> parameters;
    private final String[] sortedParameterKeys;
    private final RubyString target;
    private static final Logger LOGGER = LogManager.getLogger(FetchUpdate.class);

    public FetchUpdate(String id, final RubyString statement, final RubyHash rubyParameters, final RubyString target) {
        this.lookup_id = id;
        this.target = target;
        final String[] localStatement = {statement.toString()};
        final IRubyObject[] pairs = rubyParameters.to_a().toJavaArray();
        this.parameters = new HashMap<>(pairs.length);
        final List<Map.Entry<String, Integer>> positions = new ArrayList<>(pairs.length);
        Arrays.stream(pairs).map(pair -> (RubyArray) pair).forEach(inner -> {
            final String key = inner.first().asJavaString();
            final RubyString rubyVal = (RubyString) inner.last();
            final String symbolKey = ':' + key;
            final int pos = localStatement[0].indexOf(symbolKey);
            if (pos >= 0) {
                positions.add(new HashMap.SimpleImmutableEntry<>(key, pos));
                localStatement[0] = localStatement[0].replace(symbolKey, "?");
            }
            parameters.put(key, sprintOrGetifier(rubyVal));
        });
        this.statement = localStatement[0];
        positions.sort(Comparator.comparing(Map.Entry::getValue));
        this.sortedParameterKeys = positions.stream().map(Map.Entry::getKey).toArray(String[]::new);
    }

    private static Fetchifier sprintOrGetifier(final RubyString rubyVal) {
        final String v = rubyVal.toString();
        final int rCurly = v.indexOf('}');
        final int pctLCurly = v.indexOf("%{");
        if (pctLCurly >= 0 && rCurly >= 0 && rCurly > pctLCurly) {
            return new Sprintifier(rubyVal);
        }
        return new Getifier(rubyVal);
    }

    @Override
    public final void fetchAndUpdate(final ThreadContext ctx, final PoolDataSource pool, final IRubyObject event, final LookupFailures lookupFailures) {
        final Ruby ruby = ctx.runtime;
        final JrubyEventExtLibrary.RubyEvent rubyEvent = (JrubyEventExtLibrary.RubyEvent) event;
        try (final Connection conn = pool.getConnection()) {
            try (final PreparedStatement ps = conn.prepareStatement(this.statement, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                mapParameterArgs(ctx, (JrubyEventExtLibrary.RubyEvent) event, ps, lookupFailures);
                if (lookupFailures.parametersAreValid()) {
                    final ResultSet rs = ps.executeQuery();
                    final RubyArray array = ruby.newArray();
                    final ResultSetMetaData meta = rs.getMetaData();
                    final int columnCount = meta.getColumnCount();
                    while (rs.next()) {
                        final RubyHash hash = RubyHash.newSmallHash(ruby);
                        for (int i = 0; i < columnCount; i++) {
                            final ConvertResult result = TypeConverter.convertToRuby(ruby, meta, i + 1, rs);
                            if (result.isFailure()) {
                                lookupFailures.invalidColumnPush(result.getMessage());
                            } else {
                                hash.fastASetCheckString(ruby, ruby.newString(result.getField().toLowerCase(Locale.ENGLISH)), result.getValue());
                            }
                        }
                        if (!hash.isEmpty()) {
                            array.append(hash);
                        }
                    }
                    rubyEvent.ruby_set_field(ctx, this.target, array);
                }
            }
        } catch (final SQLException e) {
            final String eol = System.getProperty("line.separator");
            final StringBuilder sb = new StringBuilder("A SQLException occurred in lookup id:'");
            sb.append(lookup_id).append("', the error message and backtrace are:");
            sb.append(eol).append(e.getMessage());
            Arrays.stream(e.getStackTrace()).limit(10L).forEach(ste -> sb.append(eol).append(ste.toString()));
            LOGGER.error(sb.toString());
            throw new RaiseException(ruby, ruby.getStandardError(), "A SQLException occurred, it has been logged already", true);
        }
    }

    private void mapParameterArgs(final ThreadContext ctx, final JrubyEventExtLibrary.RubyEvent event, final PreparedStatement ps, final LookupFailures lookupFailures) throws SQLException {
        final String[] sortedParameterKeys1 = this.sortedParameterKeys;
        for (int i = 0; i < sortedParameterKeys1.length; i++) {
            final Fetchifier fetcher = this.parameters.get(sortedParameterKeys1[i]);
            if (fetcher != null) {
                final IRubyObject fetched = fetcher.fetch(ctx, event, lookupFailures);
                if (lookupFailures.parametersAreInvalid()) {
                    return;
                }
                ParameterConverter.convertRubyForPreparedStatement(i + 1, ps, fetched);
            }
        }
    }
}
