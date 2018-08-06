package org.logstash.jdbc_static;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
import org.jruby.ext.bigdecimal.RubyBigDecimal;
import org.logstash.ext.JrubyTimestampExtLibrary;

import java.lang.reflect.Method;
/*
  JUnit creates a new instance per test method
  This class aims to load the Ruby side Logstash module and class definitions once only
  Due to the changes in Logstash core, JrubyTimestampExtLibrary is no longer a JRuby Library with a load
  method.
  I used reflection to find and get the Ruby side loaded in newer and older versions of LS core.
  I would like to know if there is a better mechanism.
 */
public class TestCommon {
    public static Ruby ruby;

    public static boolean isLogstashTimestampLoaded(Ruby ruby) {
        RubyModule ls = ruby.getModule("LogStash");
        if (ls != null) {
            RubyClass ts = ruby.getClass("Timestamp");
            if (ts != null) {
                return true;
            }
        }
        return false;
    }

    public static boolean loadViaRubyUtil(Ruby ruby) {
        try {
            Class.forName("org.logstash.RubyUtil");
            return isLogstashTimestampLoaded(ruby);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void loadRubyBigDecimal(Ruby ruby) {
        RubyClass bd = ruby.getClass("BigDecimal");
        if (bd == null) {
            RubyBigDecimal.createBigDecimal(ruby);
        }
    }

    public static boolean loadViaLibraryLoad(Ruby ruby) {
        try {
            JrubyTimestampExtLibrary lib = new JrubyTimestampExtLibrary();
            final Method loadMethod = JrubyTimestampExtLibrary.class.getMethod("load", Ruby.class, boolean.class);
            loadMethod.invoke(lib, ruby, true);
            return isLogstashTimestampLoaded(ruby);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
