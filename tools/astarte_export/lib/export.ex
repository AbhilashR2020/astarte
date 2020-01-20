#
# This file is part of Astarte.
#
# Copyright 2019 Ispirata Srl
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

defmodule Astarte.Export do
  alias Astarte.Export.FetchData
  alias Astarte.Export.XMLGenerate
  require Logger

  @moduledoc """
    This  module provide API functions to export realm device 
    data in a xml format. This data can be used by astarte_import 
    application utlity  to import into a new realm.
  """

  @doc """
  The export_realm_data/2 function required 2 arguments to export 
  the realm data into XML format.
  the arguments are
   -realm-name -> This is a string format of input
   - path      -> path where to export the realm file.

  @spec export_realm_data(String.t, String.t) :: :ok | {:error, :invalid_parameters} | {:error, reason}

  """

  def export_realm_data(realm, path) do
    with true <- File.dir?(path) do
      timestamp = format_time
      filename = path <> "/" <> realm <> "_" <> timestamp <> ".xml"
      generate_xml(realm, filename)
    else
      result -> {:error, :invalid_parameters}
    end
  end

  defp generate_xml(realm, file) do
    Logger.info("Export started .", realm: realm, tag: "export_started")

    with {:ok, fd} = File.open(file, [:write]),
         {:ok, state} <- XMLGenerate.xml_write(:default_header,fd),
         {:ok, state} <- XMLGenerate.xml_write(:start_tag, fd, {"astarte",[]},state),
         {:ok, state} <- XMLGenerate.xml_write(:start_tag, fd, {"devices",[]},state),  
         {:ok, conn} <- FetchData.db_connection_identifier(),
         {:ok, :finished, state} <- generate_xml(conn, realm, fd, [], state) do
         {:ok, state} = XMLGenerate.xml_write(:end_tag, fd, state)
         {:ok, state} = XMLGenerate.xml_write(:end_tag, fd, state)
         File.close(fd)
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp generate_xml(conn, realm, fd, options, state) do
    with {:more_data, device_data, updated_options} <-
      FetchData.fetch_device_data(conn, realm, options),
      mapped_device_data = FetchData.process_device_data(device_data) do
      {:ok, state} = construct_device_xml_tags(mapped_device_data, fd, state)
      {:ok, interfaces} = FetchData.get_interface_details(conn, realm, device_data)
      state = process_interfaces(conn, realm, fd, state, interfaces)
      {:ok, state} = XMLGenerate.xml_write(:end_tag, fd, state)
      generate_xml(conn, realm, fd, updated_options, state)
    else
      {:ok, :completed} ->
        Logger.info("Export Completed.", realm: realm, tag: "export_completed")
        {:ok, :finished, state}
      {:error, reason} ->
        {:error, reason}
    end
  end

  def construct_device_xml_tags(device_data, fd, state) do
    %{
      device: device,
      protocol: protocol,
      registration: registration,
      credentials: credentials,
      stats: stats
    } = device_data

    {:ok, state} = XMLGenerate.xml_write(:start_tag, fd, {"device", device}, state)
    {:ok, state} = XMLGenerate.xml_write(:empty_element, fd, {"protocol", protocol, []}, state)
    {:ok, state} = XMLGenerate.xml_write(:empty_element, fd, {"registration", registration, []}, state)
    {:ok, state} = XMLGenerate.xml_write(:empty_element, fd, {"credentials", credentials, []}, state)
    {:ok, state} = XMLGenerate.xml_write(:empty_element, fd, {"stats", stats, []}, state)
  end

  def process_interfaces(conn, realm, fd, state, interfaces) do
    {:ok, state} = XMLGenerate.xml_write(:start_tag, fd, {"interfaces", []}, state)
    table_page_sizes = Application.get_env(:xandra, :cassandra_table_page_sizes)
    state = 
    Enum.reduce(interfaces, state, fn interface_details, state ->
      %{
        interface: interface_attributes,
        interface_id: interface_id,
        device_id: device_id,
        aggregation: aggregation,
        storage: storage,
        interface_type: interface_type,
        mappings: mappings
      } = interface_details
      
      {:ok, state} = XMLGenerate.xml_write(:start_tag, fd, {"interface", interface_attributes}, state) 

      state =
        case interface_type do
          :datastream ->
            case aggregation do
              :object ->
                page_size = Keyword.get(table_page_sizes, :object_datastreams)
                process_object_datastreams(conn, realm, fd, state, mappings, device_id, storage,[page_size: page_size])
                
              :individual ->
                page_size = Keyword.get(table_page_sizes, :individual_datastreams)
                process_individual_datastreams(
                  conn,
                  realm,
                  fd,
                  state,
                  mappings,
                  device_id,
                  interface_id,
                  [page_size: page_size]
                )
            end

          :properties ->
            page_size = Keyword.get(table_page_sizes, :individual_properties)
            process_individual_properties(
              conn,
              realm,
              fd,
              state,
              mappings,
              device_id,
              interface_id,
              [page_size: page_size]
            )
        end
      {:ok, state} = XMLGenerate.xml_write(:end_tag, fd, state) 
      state
    end)
    {:ok, state} = XMLGenerate.xml_write(:end_tag, fd, state)
    state
  end


  defp process_object_datastreams(
         conn,
         realm,
         fd,
         state,
         mappings,
         device_id,
         storage,
         opts
       ) do
    [h | _t] = mappings
    fullpath = h.endpoint
    [_, endpointprefix, _] = String.split(fullpath, "/")
    path = "/" <> endpointprefix
    
    sub_paths_info =
      Enum.reduce(mappings, [], fn mapping, acc1 ->
        path = mapping.endpoint
        [_, _, suffix] = String.split(path, "/")
        data_type = mapping.value_type
        [%{suffix_path: suffix, data_type: data_type} | acc1]
      end)

    {:ok, state} = XMLGenerate.xml_write(:start_tag, fd, {"datastream", [path: path]}, state) 

    fetch_object_datastreams(
      conn,
      realm,
      fd,
      state,
      path,
      sub_paths_info, 
      device_id,
      storage,
      opts
    )

   {:ok, state} = XMLGenerate.xml_write(:end_tag, fd, state) 
   state
  end

  defp process_individual_datastreams(_, _, _, state, [], _, _, _) do
    state
  end

  defp process_individual_datastreams(
         conn,
         realm,
         fd,
         state,
         [mapping | rem_mappings],
         device_id,
         interface_id,
         opts
       ) do

    {:ok, state} = XMLGenerate.xml_write(:start_tag, fd, {"datastream", [path: mapping.endpoint]}, state) 

    {:ok, :completed, state} =
      fetch_individual_datastreams(
        conn,
        realm,
        fd,
        state,
        mapping,
        device_id,
        interface_id,
        opts
      )
     
    {:ok, state} = XMLGenerate.xml_write(:end_tag, fd, state) 
    
    process_individual_datastreams(
      conn,
      realm,
      fd,
      state,
      rem_mappings,
      device_id,
      interface_id,
      opts
    )
  end

  defp process_individual_properties(_, _, _, state, [], _, _, _) do
    state
  end

  defp process_individual_properties(
         conn,
         realm,
         fd,
         state,
         [mapping | rem_mappings],
         device_id,
         interface_id,
         opts
       ) do
    {:ok, :completed, state} =
      fetch_individual_properties(
        conn,
        realm,
        fd,
        state,
        mapping,
        device_id,
        interface_id,
        opts
      )


    process_individual_properties(
      conn,
      realm,
      fd,
      state,
      rem_mappings,
      device_id,
      interface_id,
      opts
    )
  end

  defp fetch_object_datastreams(
         conn,
         realm,
         fd,
         state,
         path,
         sub_paths_info,
         device_id,
         storage,
         opts
       ) do
    with {:more_data, data, updated_options} <-
      FetchData.fetch_object_datastreams(conn, realm, path, sub_paths_info, device_id, storage, opts) do
      state = generate_object_datastream_xml(fd, state, data) 
      paging_state = Keyword.get(updated_options, :paging_state)
      case paging_state do
        nil ->
          {:ok, :completed, state}
        _ ->
          fetch_object_datastreams(
            conn,
            realm,
            fd, 
            state,
            path,
            sub_paths_info,
            device_id,
            storage,
            updated_options
          )
      end
    else
      {:ok, :completed} -> {:ok, :completed, state}
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp fetch_individual_datastreams(
         conn,
         realm,
         fd,
         state,
         mapping,
         device_id,
         interface_id,
         opts
       ) do
    with {:more_data, data, updated_options} <-
      FetchData.fetch_individual_datastreams(
             conn,
             realm,
             mapping,
             device_id,
             interface_id,
             opts
           ) do
      state = generate_individual_datastream_xml(fd, state, data)
      paging_state = Keyword.get(updated_options, :paging_state)

      case paging_state do
        nil ->
          {:ok, :completed, state}

        _ ->
          fetch_individual_datastreams(
            conn,
            realm,
            fd,
            state,
            mapping,
            device_id,
            interface_id,
            updated_options
          )
      end
    else
      {:ok, :completed} -> {:ok, :completed, state}
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp fetch_individual_properties(
         conn,
         realm,
         fd,
         state,
         mapping,
         device_id,
         interface_id,
         opts
       ) do
    with {:more_data, data, updated_options} <-
           FetchData.fetch_individual_properties(
             conn,
             realm,
             mapping,
             device_id,
             interface_id,
             opts
           ) do
      state = generate_individual_properties_xml(fd, state, data)
      paging_state = Keyword.get(updated_options, :paging_state)

      case paging_state do
        nil ->
          {:ok, :completed, state}

        _ ->
          fetch_individual_properties(
            conn,
            realm,
            fd,
            state,
            mapping,
            device_id,
            interface_id,
            updated_options
          )
      end
    else
      {:ok, :completed} -> {:ok, :completed, state}
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp format_time() do
    {{year, month, date}, {hour, minute, second}} = :calendar.local_time()

    to_string(year) <>
      "_" <>
      to_string(month) <>
      "_" <>
      to_string(date) <>
      "_" <>
      to_string(hour) <>
      "_" <>
      to_string(minute) <>
      "_" <>
      to_string(second)
  end

  def generate_individual_datastream_xml(_, state, []) do
    state
  end

  def generate_individual_datastream_xml(fd, state, [h | t]) do
    %{value: value, attributes: attributes} = h
    {:ok, state} = XMLGenerate.xml_write(:full_element, fd, {"value", attributes, value}, state) 
    generate_individual_datastream_xml(fd, state, t)
  end

  def generate_individual_properties_xml(_, state, []) do
    state
  end

  def generate_individual_properties_xml(fd, state, [h | t]) do
    %{value: value, attributes: attributes} = h
    {:ok, state} = XMLGenerate.xml_write(:full_element, fd, {"property", attributes, value}, state)
    generate_individual_datastream_xml(fd, state, t)
  end

  def generate_object_datastream_xml(_,state,[]) do
    state
  end

  def generate_object_datastream_xml(fd, state, [h | t]) do
    %{attributes: attributes, value: value} = h
    {:ok, state} = XMLGenerate.xml_write(:start_tag, fd, {"object", attributes}, state) 
    state = generate_object_item_xml(fd, state, value)
    {:ok, state} = XMLGenerate.xml_write(:end_tag, fd, state)
    generate_object_datastream_xml(fd, state, t)
  end

  def generate_object_item_xml(_, state, []) do
    state
  end

  def generate_object_item_xml(fd, state, [h | t]) do
    %{attributes: attributes, value: value} = h
    {:ok, state} = XMLGenerate.xml_write(:full_element, fd, {"item", attributes, value}, state) 
    generate_object_item_xml(fd, state, t)
  end

end
