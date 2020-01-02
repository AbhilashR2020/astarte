defmodule Astarte.Export.FetchData.Queries do
  alias Xandra
  alias Astarte.Import.LogFmtFormatter
  alias Astarte.Core.InterfaceDescriptor 
  alias Astarte.Core.Mapping
  require Logger

  def get_connection() do
    nodes = Application.get_env(:cqerl, :cassandra_nodes)
    {host, port} = Enum.random(nodes)
    Logger.info("Connecting to #{host}:#{port} cassandra database.")

    {:ok, xandra_conn} = Xandra.start_link(nodes: ["#{host}:#{port}"], atom_keys: true)
    Logger.info("Connected to database.")

    {:ok, xandra_conn}
  end

  def retrive_interface_row(conn, realm, interface, major_version) do

    interface_statement = """
    SELECT name, major_version, minor_version, interface_id, type, ownership, aggregation,
      storage, storage_type, automaton_transitions, automaton_accepting_states
    FROM #{realm}.interfaces
    WHERE name=? AND major_version=?
    """

    params = [{"ascii", interface}, 
              {"int", major_version}
             ]
 
    options = [uuid_format: :binary, 
               consistency: :quorum
              ]

    with {:ok, result} <- Xandra.execute(conn, interface_statement, params, options) do
      {:ok, result}
    else
      {:error, %Xandra.Error{message: message} = err} ->
        # Logger.error("database error: #{message}.", log_metadata(realm, device_id, err))
        {:error, :database_error}

      {:error, %Xandra.ConnectionError{} = err} ->
        # Logger.error("database connection error.", log_metadata(realm, device_id, err))
        {:error, :database_error}
    end
  end

  def fetch_interface_descriptor(conn, realm, interface, major_version) do
    with {:ok, interface_row} <- retrive_interface_row(conn, realm, interface, major_version) do
      interface_row
      |> Enum.to_list
      |> hd
      |> InterfaceDescriptor.from_db_result      
    end
  end

  def stream_devices(conn, realm, options) do
    devices_statement = """
      SELECT * from #{realm}.devices 
      """
    params = []

    query_options =
      [date_format: :integer, 
       uuid_format: :binary] 
      ++
      with value when is_integer(value) <- Keyword.get(options, :page_size) do
        with nil <- Keyword.get(options, :paging_state) do
          [page_size: value]
        else
          paging_value -> [page_size: 1, paging_state: paging_value]
        end
      end

    with {:ok, result} = Xandra.execute(conn, devices_statement, params, query_options) do
      {:ok, result}
    else
      {:error, %Xandra.Error{message: message} = err} ->
        # Logger.error("database error: #{message}.", log_metadata(realm, device_id, err))
        {:error, :database_error}

      {:error, %Xandra.ConnectionError{} = err} ->
        # Logger.error("database connection error.", log_metadata(realm, device_id, err))
        {:error, :database_error}
    end
  end

  def fetch_interface_mappings(conn, realm, interface_id) do
    mappings_statement = """
    SELECT endpoint, value_type, reliability, retention, database_retention_policy,
      database_retention_ttl, expiry, allow_unset, explicit_timestamp, endpoint_id, interface_id
    FROM #{realm}.endpoints
    WHERE interface_id=?
    """
    params = [{"uuid", interface_id}]

    options = [uuid_format: :binary]

    with {:ok, result} = Xandra.execute(conn, mappings_statement, params, options) do
      mappings = 
      result
      |> Enum.to_list
      
      mappings_1 = Enum.map(mappings, &Mapping.from_db_result!/1) 
      {:ok, mappings_1}
    else
      {:error, %Xandra.Error{message: message} = err} ->
        # Logger.error("database error: #{message}.", log_metadata(realm, device_id, err))
        {:error, :database_error}

      {:error, %Xandra.ConnectionError{} = err} ->
        # Logger.error("database connection error.", log_metadata(realm, device_id, err))
        {:error, :database_error}
    end
  end  
end

 def retrive_individual_properties(conn, realm, device_id, interface_id, data_type) do
   properties_statement = """
   SELECT  #{data_type}, reception_timestamp , path from #{realm}.individual_properties 
     where device_id=? AND interface_id=?
   """
   
   params = [{"uuid", device_id},
             {"uuid", interface_id}] 

   options = [uuid_format: :binary,
              date_format: :integer] 
  
    with {:ok, result} = Xandra.execute(conn, properties_statement, params, options) do
      result |> Enum.to_list
    else
      {:error, %Xandra.Error{message: message} = err} ->
        # Logger.error("database error: #{message}.", log_metadata(realm, device_id, err))
        {:error, :database_error}

      {:error, %Xandra.ConnectionError{} = err} ->
        # Logger.error("database connection error.", log_metadata(realm, device_id, err))
        {:error, :database_error}
    end
  end

  def retrive_individual_datastreams(conn, realm, device_id, interface_id, endpoint_id, interface_name, data_type) do
    individual_datastream_statement = """
    SELECT #{data_type}, reception_timestamp FROM  #{realm}.individual_datastreams WHERE device_id=? AND
      interface_id=? AND endpoint_id=? AND path=?
    """
    
    params = [{"uuid", device_id},
              {"uuid", interface_id},
              {"uuid", endpoint_id},
              {"ascill", path}]
    
    options = [uuid_format: :binary,
               date_format: :integer] 

    with {:ok, result} = Xandra.execute(conn, individual_datastream_statement, params, options) do
      result |> Enum.to_list
    else
      {:error, %Xandra.Error{message: message} = err} ->
        # Logger.error("database error: #{message}.", log_metadata(realm, device_id, err))
        {:error, :database_error}
    
      {:error, %Xandra.ConnectionError{} = err} ->
        # Logger.error("database connection error.", log_metadata(realm, device_id, err))
        {:error, :database_error}
    end
  end
