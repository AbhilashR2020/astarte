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
    with {:ok, file_descriptor} = File.open(file, [:write]),
         {:ok, doc, state} <- XMLStreamWriter.new_document(),
         {:ok, header, state} <- XMLStreamWriter.start_document(state),
         {:ok, astarte_tag, state} <- XMLStreamWriter.start_element(state, "astarte", []),
         {:ok, devices_tag, state} <- XMLStreamWriter.start_element(state, "devices", []),
         xml_data <- :erlang.iolist_to_binary([doc, header, astarte_tag, devices_tag]),
         :ok <- IO.puts(file_descriptor, xml_data),
	 {:ok, conn} <- FetchData.db_connection_identifier(),
         {:ok, :finished} <- generate_xml(conn, realm, file_descriptor, [], state) do
      File.close(file_descriptor)
    else
      {:error, reason} ->
        {:error, reason}
    end
  end


  defp generate_xml(conn, realm, file_descriptor, opts, state) do
    with {:more_data, device_data, updated_options} <- FetchData.fetch_device_data(conn, realm, opts), 
      mapped_device_data = FetchData.process_device_data(device_data) do
      {:ok, xml_data, state} = construct_device_xml_tags(mapped_device_data,state)
      IO.puts(file_descriptor, xml_data)
      {:ok, interfaces} = FetchData.get_interface_details(conn, realm, device_data)
      state = process_interfaces(conn, realm, file_descriptor, state, interfaces)
      {:ok, device_end_tag, state} = XMLStreamWriter.end_element(state)
      xml_data = :erlang.iolist_to_binary([device_end_tag])
      IO.puts(file_descriptor, xml_data) 
      generate_xml(conn, realm, file_descriptor, updated_options, state)
    else
      {:ok, :completed} ->
        Logger.info("Export Completed.", realm: realm, tag: "export_completed")
        {:ok, :finished}
      {:error, reason} -> {:error, reason}
    end
  end
  
 
  def construct_device_xml_tags(device_data, state) do
    %{device: device,
      protocol: protocol,
      registration: registration,
      credentials: credentials,
      stats: stats} = device_data
    {:ok, device_tag, state} = XMLStreamWriter.start_element(state, "device", device)
    {:ok, protocol_tag, state} = XMLStreamWriter.empty_element(state, "protocol", protocol)
    {:ok, registration_tag, state} = XMLStreamWriter.empty_element(state, "registration", registration) 
    {:ok, credentials_tag, state} = XMLStreamWriter.empty_element(state, "credentials", credentials)
    {:ok, stats_tag, state} = XMLStreamWriter.empty_element(state, "stats", stats)
    xml_data = :erlang.iolist_to_binary([device_tag, protocol_tag, registration_tag, credentials_tag, stats_tag]) 
    {:ok, xml_data, state} 
  end
  
  def process_interfaces(conn, realm, file_descriptor, state, interfaces) do
    {:ok, interfaces_tag, state} = XMLStreamWriter.start_element(state, "interfaces", [])
    xml_data = :erlang.iolist_to_binary([interfaces_tag])
    IO.puts(file_descriptor, xml_data)
    Enum.reduce(interfaces, state, fn interface_details, state ->
      %{interface: interface_attributes,
        interface_id: interface_id,
        device_id: device_id,
        aggregation: aggregation,
        storage: storage,
        interface_type: interface_type,
        mappings: mappings} = interface_details
        {:ok, interface_tag, state} = XMLStreamWriter.start_element(state, "interface", interface_attributes)
        xml_data = :erlang.iolist_to_binary([interface_tag])
        IO.puts(file_descriptor, xml_data)
        state = 
        case interface_type do
          :datastream ->
             case aggregation do
               :object ->
                  #process_object_datastreams(conn, realm, file_descriptor, state, mappings, device_id, storage,[])
                  state 
               :individual ->
                  process_individual_datastreams(conn, realm, file_descriptor, state, mappings, device_id, interface_id,[])
             end
          :properties ->
            process_individual_properties(conn, realm, file_descriptor, state, mappings, device_id, interface_id,[])
        end
        {:ok, end_tag, state}  = XMLStreamWriter.end_element(state)
        xml_data = :erlang.iolist_to_binary([end_tag])
        IO.puts(file_descriptor, xml_data) 
        state
    end)
    {:ok, end_tag, state} = XMLStreamWriter.end_element(state)
    xml_data = :erlang.iolist_to_binary([end_tag])
    IO.puts(file_descriptor, xml_data)
    state
  end  
  
  
  defp process_object_datastreams(_, _, _, state, [], _, _, _) do
    state
  end
  
  defp process_object_datastreams(conn, realm, file_descriptor, state, [mapping |rem_mappings], device_id, storage, opts) do
    fetch_object_datastreams(conn, realm, file_descriptor, state, mapping, device_id, storage, opts)
    process_object_datastreams(conn, realm, file_descriptor, state, rem_mappings, device_id, storage, opts)
  end
  
  
  defp process_individual_datastreams(_, _, _, state, [], _, _, _) do
    state
  end
  
  defp process_individual_datastreams(conn, realm, file_descriptor, state, [mapping |rem_mappings], device_id, interface_id, opts) do
     {:ok, datastream_start_tag, state} = XMLStreamWriter.start_element(state, "datastream", [path: mapping.endpoint]) 
     xml_data = :erlang.iolist_to_binary([datastream_start_tag])
     IO.puts(file_descriptor, xml_data) 
     {:ok, :completed, state} = fetch_individual_datastreams(conn, realm, file_descriptor, state, mapping, device_id, interface_id, opts)
     {:ok, datastream_end_tag, state} = XMLStreamWriter.end_element(state)
     xml_data = :erlang.iolist_to_binary([datastream_end_tag])
     IO.puts(file_descriptor, xml_data) 
     process_individual_datastreams(conn, realm, file_descriptor, state, rem_mappings, device_id, interface_id, opts)
  end


  defp process_individual_properties(_, _, _, state, [], _, _, _) do
    state 
  end
  
  defp process_individual_properties(conn, realm, file_descriptor, state, [mapping |rem_mappings], device_id, interface_id, opts) do
    {:ok, :completed, state} = fetch_individual_properties(conn, realm, file_descriptor, state, mapping, device_id, interface_id, opts)
    IO.inspect state
    process_individual_properties(conn, realm, file_descriptor, state, rem_mappings, device_id, interface_id, opts)	
  end
  
  
  defp fetch_object_datastreams(conn, realm, file_descriptor, state, mapping, device_id, storage, opts) do
    with {:more_data, data, updated_options} <- 
	 FetchData.fetch_object_datastreams(conn, realm, mapping, device_id, storage, opts) do
       paging_state = Keyword.get(updated_options, :paging_state)
       case paging_state do
           nil ->
              {:ok, :completed, state}
           _ ->
              FetchData.fetch_object_datastreams(conn, realm, mapping, device_id, storage, updated_options)
       end 
    else
      {:ok, :completed} -> :ok
      {:error, reason} -> {:error, reason, state}
    end
  end
  
  defp fetch_individual_datastreams(conn, realm, file_descriptor, state, mapping, device_id, interface_id, opts) do
    with {:more_data, data, updated_options} <- 
      FetchData.fetch_individual_datastreams(conn, realm, mapping, device_id, interface_id, opts) do
      state = generate_individual_datastream_xml(file_descriptor,state, data) 
      paging_state = Keyword.get(updated_options, :paging_state)
      case paging_state do
        nil ->
          {:ok, :completed, state}
        _ ->
          fetch_individual_datastreams(conn, realm, file_descriptor, state, mapping, device_id, interface_id, updated_options)
      end     
    else
      {:ok, :completed} -> {:ok, :completed, state}
      {:error, reason} -> {:error, reason, state}
    end
  end
  
  defp fetch_individual_properties(conn, realm, file_descriptor, state, mapping, device_id, interface_id, opts) do
    with {:more_data, data, updated_options} <- FetchData.fetch_individual_properties(conn, realm, mapping, device_id, interface_id, opts) do
      state = generate_individual_properties_xml(file_descriptor,state, data)
      paging_state = Keyword.get(updated_options, :paging_state)
      case paging_state do
        nil ->
          {:ok, :completed, state}
        _ ->
          fetch_individual_properties(conn, realm, file_descriptor, state, mapping, device_id, interface_id, updated_options)
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

  def generate_individual_datastream_xml(_, state,[]) do
    state
  end 
 
  def generate_individual_datastream_xml(file_descriptor, state, [h | t]) do
    %{value: value, attributes: attributes} = h
    {:ok, start_tag, state}  = XMLStreamWriter.start_element(state, "value", attributes)
    {:ok, data, state}  = XMLStreamWriter.characters(state, value)
    {:ok, end_tag, state}  = XMLStreamWriter.end_element(state)
    xml_data = :erlang.iolist_to_binary([start_tag, data, end_tag])
    IO.puts(file_descriptor, xml_data) 
    generate_individual_datastream_xml(file_descriptor, state, t)   
  end

  def generate_individual_properties_xml(_, state,[]) do
    state
  end

  def generate_individual_properties_xml(file_descriptor, state, [h | t]) do
    %{value: value, attributes: attributes} = h
    {:ok, start_tag, state}  = XMLStreamWriter.start_element(state, "property", attributes)
    {:ok, data, state}  = XMLStreamWriter.characters(state, value)
    {:ok, end_tag, state}  = XMLStreamWriter.end_element(state)
    xml_data = :erlang.iolist_to_binary([start_tag, data, end_tag])
    IO.puts(file_descriptor, xml_data)
    generate_individual_datastream_xml(file_descriptor, state, t)
  end
end
