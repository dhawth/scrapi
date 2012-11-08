default.threecrowd[:cassandra_stats][:keyspace] = "stats"

default.threecrowd[:scrapi][:service][:username]  = 'scrapi'
default.threecrowd[:scrapi][:service][:groupname] = 'scrapi'

default.threecrowd[:scrapi][:logging_config][:log_level] = 'INFO'
default.threecrowd[:scrapi][:logging_config][:log_dir] = "/var/log/scrapi"
default.threecrowd[:scrapi][:logging_config][:gc_log_enable] = true

default.threecrowd[:scrapi][:listen_port] = 9912

default.threecrowd[:scrapi][:statsd][:host] = 'localhost'
default.threecrowd[:scrapi][:statsd][:port] = 8_125
default.threecrowd[:scrapi][:statsd][:period] = 10000 #milliseconds
default.threecrowd[:scrapi][:statsd][:prepend_strings] = ['"3crowd.stats_system.scrapi"']

###
### -- These are defined in the environment attribute overrides
###
default.threecrowd[:scrapi][:cassy_servers] = Array.new
