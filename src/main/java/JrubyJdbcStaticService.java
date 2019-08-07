import org.jruby.Ruby;
import org.jruby.runtime.load.BasicLibraryService;
import org.logstash.ext.JdbcStaticLibrary;

public class JrubyJdbcStaticService implements BasicLibraryService {
    @Override
    public final boolean basicLoad(final Ruby runtime) {
        new JdbcStaticLibrary().load(runtime, false);
        return true;
    }
}
