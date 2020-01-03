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
        Enum.reduce(mappings, [], fn mapping, acc1 ->
          endpoint_id = mapping.endpoint_id
          path = mapping.endpoint
          data_type = mapping.value_type

          values =
            case interface_type do
              :datastream ->
                case aggregation do
                  :individual ->
                    fetch_individual_datastream_values(
                      conn,
                      realm,
                      device_id,
                      interface_id,
                      endpoint_id,
                      path,
                      data_type
                    )

                  :object ->
                    fetch_object_datastream_value(
                      conn,
                      realm,
                      storage,
                      device_id,
                      path,
                      data_type
                    )
                end

              :properties ->
                fetch_individual_properties_values(
                  conn,
                  realm,
                  device_id,
                  interface_id,
                  data_type
                )
            end

          case values do
            [] ->
              acc1

            _ ->
              [%{:path => path, :type => {interface_type, aggregation}, :value => values} | acc1]
          end
        end)

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

  defp fetch_individual_datastream_values(
         conn,
         realm,
         device_id,
         interface_id,
         endpoint_id,
         path,
         data_type
       ) do
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

        return_value =
          map[atom_data_field]
          |> to_charlist

        reception_timestamp =
          map[:reception_timestamp]
          |> DateTime.from_unix!(:millisecond)
          |> DateTime.to_iso8601()

        Map.replace!(map, atom_data_field, return_value)
        |> Map.replace!(:reception_timestamp, reception_timestamp)
      end)
  end

  defp fetch_object_datastream_value(conn, realm, storage, device_id, path, data_field) do
    {:ok, result} =
      Queries.retrive_object_datastream_value(conn, realm, storage, device_id, path, data_field)

    values =
      Result.all_rows(result)
      |> Enum.map(fn map ->
        reception_timestamp =
          map[:reception_timestamp]
          |> DateTime.from_unix!(:millisecond)
          |> DateTime.to_iso8601()

        v_realpathdatavalue =
          map[:v_realpathdatavalue]
          |> Kernel.to_string()

        Map.replace!(map, :reception_timestamp, reception_timestamp)
        |> Map.replace!(:v_realpathdatavalue, v_realpathdatavalue)
      end)
  end

  defp fetch_individual_properties_values(conn, realm, device_id, interface_id, data_field) do
    result =
      Queries.retrive_individual_properties(conn, realm, device_id, interface_id, data_field)

    values =
      Result.all_rows(result)
      |> Enum.map(fn map ->
        reception_timestamp =
          map[:reception_timestamp]
          |> DateTime.from_unix!(:millisecond)
          |> DateTime.to_iso8601()

        v_realpathdatavalue =
          map[:v_realpathdatavalue]
          |> Kernel.to_string()

        Map.replace!(map, :reception_timestamp, reception_timestamp)
        |> Map.replace!(:v_realpathdatavalue, v_realpathdatavalue)
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
end
