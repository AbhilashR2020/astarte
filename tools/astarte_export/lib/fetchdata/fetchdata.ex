defmodule Astarte.Export.FetchData do
  alias Astarte.Core.Device
  alias Astarte.Core.Mapping
  alias Astarte.Core.Mapping.EndpointsAutomaton
  alias Astarte.DataAccess.Database
  alias Astarte.DataAccess.Interface
  alias Astarte.DataAccess.Mappings
  alias Astarte.Export.FetchData.Queries
  require Logger

  defmodule State do
    defstruct [
      :device_id,
      :revision,
      :pending_empty_cache,
      :secret_bcrypt_hash,
      :first_registration,
      :inhibit_request,
      :cert_aki,
      :cert_serial,
      :first_credentials_request,
      :last_credentials_request_ip,
      :total_received_msgs,
      :total_received_bytes,
      :last_connection,
      :last_disconnection,
      :last_seen_ip,
      :interfaces
    ]
  end

  @spec process_device_data(identifier(), String.t(), list()) :: struct()

  def process_device_data(conn, realm, device_data) do
    #    device_id =
    #      device_data[:device_id]
    #      |> Device.encode_device_id()
    device_id = 0
    revision = device_data[:protocol_revision]

    pending_empty_cache =
      device_data[:pending_empty_cache]
      |> to_string
      |> String.downcase()

    secret_bcrypt_hash = device_data[:credentials_secret]

    first_registration =
      device_data[:first_registration]
      |> DateTime.from_unix!(:millisecond)
      |> DateTime.to_iso8601()

    inhibit_request =
      device_data[:inhibit_credentials_request]
      |> to_string
      |> String.downcase()

    cert_serial = device_data[:cert_serial]

    cert_aki = device_data[:cert_aki]

    first_credentials_request =
      device_data[:first_credentials_request]
      |> DateTime.from_unix!(:millisecond)
      |> DateTime.to_iso8601()

    last_credentials_request_ip =
      device_data[:last_credentials_request_ip]
      |> :inet_parse.ntoa()
      |> to_string()

    total_received_msgs =
      device_data[:total_received_msgs]
      |> to_string()

    total_received_bytes =
      device_data[:total_received_bytes]
      |> to_string()

    last_connection =
      device_data[:last_connection]
      |> DateTime.from_unix!(:millisecond)
      |> DateTime.to_iso8601()

    last_disconnection =
      device_data[:last_disconnection]
      |> DateTime.from_unix!(:millisecond)
      |> DateTime.to_iso8601()

    last_seen_ip =
      device_data[:last_seen_ip]
      |> :inet_parse.ntoa()
      |> to_string()

    interface_details = gen_interface_details(conn, realm, device_data)

    %State{
      device_id: device_id,
      revision: revision,
      pending_empty_cache: pending_empty_cache,
      secret_bcrypt_hash: secret_bcrypt_hash,
      first_registration: first_registration,
      inhibit_request: inhibit_request,
      cert_aki: cert_aki,
      cert_serial: cert_serial,
      first_credentials_request: first_credentials_request,
      last_credentials_request_ip: last_credentials_request_ip,
      total_received_msgs: total_received_msgs,
      total_received_bytes: total_received_bytes,
      last_connection: last_connection,
      last_disconnection: last_disconnection,
      last_seen_ip: last_seen_ip,
      interfaces: interface_details
    }
  end

  defp gen_interface_details(conn, realm, device_data) do
    device_id = device_data[:device_id]
    introspection = device_data[:introspection]
    introspection_minor = device_data[:interospection_minor]

    Enum.reduce(introspection, [], fn {interface_name, major_version}, acc ->
      {:ok, interface_description} =
        Queries.fetch_interface_descriptor(conn, realm, interface_name, major_version)

      minor_version = interface_description.minor_version |> to_string()
      major_version1 = major_version |> Kernel.to_string()
      interface_id = interface_description.interface_id
      aggregation = interface_description.aggregation
      storage = interface_description.storage
      interface_type = interface_description.type
      {:ok, mappings} = Queries.fetch_interface_mappings(conn, realm, interface_id)

      mapped_data_fields =
        case interface_type do
          :datastream ->
            case aggregation do
              :object ->
                fetch_object_datastreams(conn, realm, mappings, device_id, storage)

              :individual ->
                fetch_individual_datastreams(conn, realm, mappings, device_id, interface_id)
            end

          :properties ->
            fetch_individual_properties(conn, realm, mappings, device_id, interface_id)
        end

      [
        %{
          :interface_name => interface_name,
          :major_version => major_version1,
          :minor_version => minor_version,
          :active => "true",
          :mappings => mapped_data_fields
        }
        | acc
      ]
    end)
  end

  def fetch_individual_datastreams(conn, realm, mappings, device_id, interface_id) do
    Enum.reduce(mappings, [], fn mapping, acc1 ->
      endpoint_id = mapping.endpoint_id
      path = mapping.endpoint
      data_type = mapping.value_type
      data_field = get_data_field_name(data_type)

      {:ok, result} =
        Queries.retrive_individual_datastreams(
          conn,
          realm,
          device_id,
          interface_id,
          endpoint_id,
          path,
          data_field
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

          Map.new()
          |> Map.put(:value, value)
          |> Map.put(:reception_timestamp, reception_timestamp)
        end)

      case values do
        [] ->
          acc1

        _ ->
          values1 =
            Map.new()
            |> Map.put(:path, path)
            |> Map.put(:value, values)

          [values1 | acc1]
      end
    end)
  end

  def fetch_object_datastreams(conn, realm, [h | _] = mappings, device_id, storage) do
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

    {:ok, result} = Queries.retrive_object_datastream_value(conn, realm, storage, device_id, path)

    values =
      Enum.to_list(result)
      |> Enum.map(fn map ->

        reception_timestamp =
          map[:reception_timestamp]
          |> DateTime.from_unix!(:millisecond)
          |> DateTime.to_iso8601()


        value_list =
          Enum.map_reduce(map, %{}, fn {key, value}, acc ->
            case to_string(key) do
              "v_" <> item ->
                match_object =
                  Enum.find(extract_2nd_level_params, fn map1 -> map1[:suffix] == item end)

                data_type = match_object[:data_type]
                token = "/" <> match_object[:suffix]
                value1 = from_native_type(value, :double)
                Map.put(acc, token, value1)

              _ ->
                acc
            end
          end)

        Map.new()
        |> Map.put(:reception_timestamp, reception_timestamp)
        |> Map.put(:path, path)
        |> Map.put(:values, value_list)
      end)
  end

  defp fetch_individual_properties(conn, realm, mappings, device_id, interface_id) do

    Enum.reduce(mappings, [], fn mapping, acc1 ->
      endpoint_id = mapping.endpoint_id
      path = mapping.endpoint
      data_type = mapping.value_type
      data_field = get_data_field_name(data_type)

      {:ok, result} =
        Queries.retrive_individual_properties(conn, realm, device_id, interface_id, data_field)


      values =
        Enum.to_list(result)
        |> Enum.map(fn map ->
          reception_timestamp =
            map[:reception_timestamp]
            |> DateTime.from_unix!(:millisecond)
            |> DateTime.to_iso8601()

          path =
            map[:path]
            |> Kernel.to_string()

          Map.replace!(map, :reception_timestamp, reception_timestamp)
          |> Map.replace!(:path, path)
        end)
    end)
  end

  def get_data_field_name(data_type) when is_atom(data_type) do
    case data_type do
      :double -> "double_value"
      :integer -> "integer_value"
      :boolean -> "boolean_value"
      :longinteger -> "longinteger_value"
      :string -> "string_value"
      :binaryblob -> "binaryblob_value"
      :datetime -> "datetime_value"
      :doublearray -> "doublearray_value"
      :integerarray -> "integerarray_value"
      :booleanarray -> "booleanarray_value"
      :longintegerarray -> "longintegerarray_value"
      :stringarray -> "stringarray_value"
      :binaryblobarray -> "binaryblobarray_value"
      :datetimearray -> "datetimearray_value"
      _ -> :error
    end
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
