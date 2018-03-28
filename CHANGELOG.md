## 1.0.2
 - Fixes for [jdbc_static filter - #18](https://github.com/logstash-plugins/logstash-filter-jdbc_static/issues/18), [jdbc_static filter - #17](https://github.com/logstash-plugins/logstash-filter-jdbc_static/issues/17), [jdbc_static filter - #12](https://github.com/logstash-plugins/logstash-filter-jdbc_static/issues/12) Use Java classloader to load driver jar. Use system import from file to loader local database. Prevent locking errors when no records returned.
 - Fix [jdbc_static filter - #8](https://github.com/logstash-plugins/logstash-filter-jdbc_static/issues/8) loader_schedule now works as designed.

## 1.0.1
 - Docs: Edit documentation

## 1.0.0
 - Initial commit
