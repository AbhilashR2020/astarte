use Mix.Config

config :xandra,
  cassandra_nodes: [{System.get_env("CASSANDRA_DB_HOST"), System.get_env("CASSANDRA_DB_PORT")}]

config :logger, :console,
  format: {PrettyLog.LogfmtFormatter, :format}, 
  metadata: [:module, :function, :device_id, :realm, interface_id, :reason, :tag]

config :logger,
  compile_time_purge_matching: [
    [level_lower_than: :info]
  ]

