defmodule Astarte.Export.FetchData do
  alias Astarte.Core.Device
  alias Astarte.Core.CQLUtils
  alias Astarte.Export.FetchData.Queries


  def db_connection_identifier() do
    with {:ok, conn_ref} <- Queries.get_connection() do
        {:ok, conn_ref}
    else
      _ -> {:error, :connection_setup_failed}
    end
  end
  
  
  def fetch_device_data(conn, realm, []) do
    fetch_device_data(conn, realm, [page_size: 1)
  end
     
  def fetch_device_data(conn, realm, opts) do
    with {:ok, page} <- Queries.stream_devices(conn, realm, opts),
         [device_data | _tail] <- Enum.to_list(page) do
      {:more_data, device_data, [paging_state: page.paging_state, page_size: 1]}
    else
      [] -> {:ok, :completed}
      {:error, reason} -> {:error, reason}
    end
  end

  
  def process_device_data(device_data) do
    device_id = Device.encode_device_id(device_data.device_id)
    revision = device_data.protocol_revision

    pending_empty_cache =
      device_data.pending_empty_cache
      |> to_string
      |> String.downcase()

    secret_bcrypt_hash = device_data.credentials_secret

    first_registration =
      device_data.first_registration
      |> DateTime.from_unix!(:millisecond)
      |> DateTime.to_iso8601()

    inhibit_request =
      device_data.inhibit_credentials_request
      |> to_string
      |> String.downcase()

    cert_serial = device_data.cert_serial
    cert_aki = device_data.cert_aki

    first_credentials_request =
      device_data.first_credentials_request
      |> DateTime.from_unix!(:millisecond)
      |> DateTime.to_iso8601()

    last_credentials_request_ip =
      device_data.last_credentials_request_ip
      |> :inet_parse.ntoa()
      |> to_string()

    total_received_msgs = to_string(device_data.total_received_msgs)
    total_received_bytes = to_string(device_data.total_received_bytes)

    last_connection =
      device_data.last_connection
      |> DateTime.from_unix!(:millisecond)
      |> DateTime.to_iso8601()

    last_disconnection =
      device_data.last_disconnection
      |> DateTime.from_unix!(:millisecond)
      |> DateTime.to_iso8601()

    last_seen_ip =
      device_data.last_seen_ip
      |> :inet_parse.ntoa()
      |> to_string()
   
    device_attributes   = [device_id: device_id]
    
    protocol_attributes = [revision: revision]

    registration_attributes = [secret_bcrypt_hash: secret_bcrypt_hash,
                               first_registration: first_registration]

    credentials_attributes = [inhibit_request: inhibit_request,
                              cert_serial: cert_serial,
                              cert_aki: cert_aki,
                              first_credentials_request: first_credentials_request,
                              last_credentials_request_ip: last_credentials_request_ip]

    stats_attributes = [total_received_msgs: total_received_msgs,
                       total_received_bytes: total_received_bytes,
                       last_connection: last_connection,
                       last_disconnection: last_disconnection,
                       last_seen_ip: last_seen_ip]
    
    %{device: device_attributes,
      protocol: protocol_attributes,
      registration: registration_attributes,
      credentials: credentials_attributes,
      stats: stats_attribtes}
  end   
   
  def get_interface_details(conn, realm, device_data) do
    device_id = device_data.device_id
    introspection = device_data.introspection
    introspection_minor = device_data.interospection_minor
    mapped_interfaces = 
    Enum.reduce(introspection, [], fn {interface_name, major_version}, acc ->
      {:ok, interface_description} =
        Queries.fetch_interface_descriptor(conn, realm, interface_name, major_version,[])

      interface_id = interface_description.interface_id
      aggregation = interface_description.aggregation
      storage = interface_description.storage
      interface_type = interface_description.type
      {:ok, mappings} = Queries.fetch_interface_mappings(conn, realm, interface_id,[])
     
      interface_attributes = [interface_name: interface_name,
                              major_version: major_version,
                              minor_version: minor_version,
                              active: "true"]
      [%{interface: interface_attributes,
         interface_id: interface_id,
		 device_id: device_id,
         aggregation: aggregation,
         storage: storage,
         interface_type: interface_type,
         mappings: mappings} | acc]
    end)
    {:ok, mapped_interfaces} 
  end


  def fetch_individual_datastreams(conn, realm, mapping, device_id, interface_id, options) do
      endpoint_id = mapping.endpoint_id
      path = mapping.endpoint
      data_type = mapping.value_type
      data_field = CQLUtils.type_to_db_column_name(data_type)

      {:ok, result} =
        Queries.retrieve_individual_datastreams(
          conn,
          realm,
          device_id,
          interface_id,
          endpoint_id,
          path,
          data_field,
          options
        )

      values =
        Enum.to_list(result)
        |> Enum.map(fn map ->
          atom_data_field = String.to_atom(data_field)
          return_value = map[atom_data_field]
          value = from_native_type(return_value, data_type)

          reception_timestamp =
            map[:reception_timestamp]
            |> DateTime.from_unix!(:millisecond)
            |> DateTime.to_iso8601()

          %{value: value, reception_timestamp: reception_timestamp}
        end)

      case values do
        [] ->
          acc1

        _ ->
          [%{path: path, aggregation: :individual, value: values} | acc1]
      end
    end)
  end

  def fetch_object_datastreams(conn, realm, [h | _] = mappings, device_id, storage, options) do
    fullpath = h.endpoint
    [_, endpointprefix, _] = String.split(fullpath, "/")
    path = "/" <> endpointprefix

    extract_2nd_level_params =
      Enum.reduce(mappings, [], fn mapping, acc1 ->
        path = mapping.endpoint
        [_, _, suffix] = String.split(path, "/")
        data_type = mapping.value_type
        [%{suffix_path: suffix, data_type: data_type} | acc1]
      end)

    {:ok, result} =
      Queries.retrieve_object_datastream_value(conn, realm, storage, device_id, path, options)

    result1 = Enum.to_list(result)

    value =
      Enum.reduce(result1, %{}, fn map, acc ->
        reception_timestamp =
          map[:reception_timestamp]
          |> DateTime.from_unix!(:millisecond)
          |> DateTime.to_iso8601()

        list = Map.to_list(map)

        value_list =
          List.foldl(list, [], fn {key, value}, acc1 ->
            with "v_" <> item <- to_string(key),
                 match_object when match_object != nil <-
                   Enum.find(extract_2nd_level_params, fn map1 -> map1[:suffix_path] == item end),
                 data_type = match_object[:data_type],
                 token = "/" <> match_object[:suffix_path],
                 value1 when value1 != "" <- from_native_type(value, data_type) do
              [%{name: token, value: value1} | acc1]
            else
              _ -> acc1
            end
          end)

        case acc == %{} do
          true ->
            %{
              path: path,
              aggregation: :object,
              value: [%{reception_timestamp: reception_timestamp, value: value_list}]
            }

          false ->
            inner_list = acc.value

            updated_list =
              [%{reception_timestamp: reception_timestamp, value: value_list}] ++ inner_list

            %{acc | value: updated_list}
        end
      end)

    [value]
  end

  defp fetch_individual_properties(conn, realm, mappings, device_id, interface_id, options) do
    Enum.reduce(mappings, [], fn mapping, acc1 ->
      endpoint_id = mapping.endpoint_id
      path = mapping.endpoint
      data_type = mapping.value_type
      data_field = CQLUtils.type_to_db_column_name(data_type)

      {:ok, result} =
        Queries.retrieve_individual_properties(conn, realm, device_id, interface_id, data_field, options)

      values =
        Enum.to_list(result)
        |> Enum.map(fn map ->
          reception_timestamp =
            map[:reception_timestamp]
            |> DateTime.from_unix!(:millisecond)
            |> DateTime.to_iso8601()

          path = map[:path] |> Kernel.to_string()

          atom_data_field = String.to_atom(data_field)
          return_value = map[atom_data_field]
          value = from_native_type(return_value, data_type)

          %{reception_timestamp: reception_timestamp, path: path, value: value}
        end)
    end)
  end

  defp from_native_type(value_chars, :double) do
    to_string(value_chars)
  end

  defp from_native_type(value_chars, :integer) do
    to_string(value_chars)
  end

  defp from_native_type(value_chars, :boolean) do
    to_string(value_chars)
  end

  defp from_native_type(value_chars, :longinteger) do
    to_string(value_chars)
  end

  defp from_native_type(value_chars, :string) do
    to_string(value_chars)
  end

  defp from_native_type(value_chars, :binaryblob) do
    base64 = to_string(value_chars)
    {:ok, binary_blob} = Base.encode64(base64)
    binary_blob
  end

  defp from_native_type(value_chars, :datetime) do
    datetime =
      value_chars
      |> DateTime.from_unix!(:millisecond)
      |> DateTime.to_iso8601()
  end

  defp from_native_type(values, expected_types) when is_map(values) and is_map(expected_types) do
    obj =
      Enum.reduce(values, %{}, fn {"/" <> key, value}, acc ->
        value_type = Map.fetch!(expected_types, key)

        {:ok, native_type} = from_native_type(value, value_type)
        Map.put(acc, key, native_type)
      end)

    {:ok, obj}
  end
end
