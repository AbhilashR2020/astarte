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
         xml_data <- :erlang.iolist_to_binary([doc, header, astrate_tag, devices_tag])
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
    with {:more_data, device_data, updated_options} <- FetchData.fetch_device_data(conn, realm, opts) do
      mapped_device_data = FetchData.process_device_data(device_data),
      {:ok, xml_data, state} = construct_device_xml_tags(mapped_device_data,state),
      :ok <- IO.puts(file_descriptor, xml_data),
      {:ok, interfaces} = FetchData.get_interface_details(conn, realm, device_data)
    else
      {:ok, :completed} ->
        tags = astarte_default_close_tags()
        IO.puts(file_descriptor, tags)
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
    xml_data <- :erlang.iolist_to_binary([device_tag, protocol_tag, registration_tag, credentials_tag, stats_tag]) 
    {:ok, xml_data, state} 
  end
  
  def process_interfaces(conn, realm, file_descriptor, state, interfaces) do
    {:ok, interfaces_tag, state} = XMLStreamWriter.start_element(state, "interfaces", [])
	Enum.reduce(interfaces_tag, state, fn interface_details, state ->
        %{interface: interface_attributes,
          interface_id: interface_id,
		  device_id: device_id,
          aggregation: aggregation,
          storage: storage,
          interface_type: interface_type,
          mappings: mappings} = interface_details,
          {:ok, interface_tag, state} = XMLStreamWriter.start_element(state, "interface", interface_attributes)
          case interface_type do
            :datastream ->
               case aggregation do
                 :object ->
                    process_object_datastreams(conn, realm, file_descriptor, state, mappings, device_id, storage,[])
                 :individual ->
                    process_individual_datastreams(conn, realm, file_descriptor, state, mappings, device_id, interface_id,[])
               end
            :properties ->
              process_individual_properties(conn, realm, file_descriptor, state, mappings, device_id, interface_id,[])
          end
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
     fetch_individual_datastreams(conn, realm, file_descriptor, state, mapping, device_id, interface_id, opts)
     process_individual_datastreams(conn, realm, file_descriptor, state, rem_mappings, device_id, interface_id, opts)
  end


  defp process_individual_properties(_, _, _, state, [], _, _, _) do
    State
  end
  
  defp process_individual_properties(conn, realm, file_descriptor, state, [mapping |rem_mappings], device_id, interface_id, opts) do
    fetch_individual_properties(conn, realm, file_descriptor, state, mapping, device_id, interface_id, opts)
    process_individual_properties(conn, realm, file_descriptor, state, rem_mappings, device_id, interface_id, opts)	
  end
  
  
  defp fetch_object_datastreams(conn, realm, file_descriptor, state, mapping, device_id, storage, opts) do
    with {:more_data, data, updated_options} <- 
	 FetchData.fetch_object_datastreams(conn, realm, mapping, device_id, storage, options) do

    else
      {:ok, :completed} -> 
      {:error, reason} -> {:error, reason, state}
    end
  end
  
  defp fetch_individual_datastreams(conn, realm, file_descriptor, state, mapping, device_id, interface_id, opts) do
    with {:more_data, data, updated_options} <- 
	  FetchData.fetch_individual_datastreams(conn, realm, mapping, device_id, interface_id, opts) do
	  

    else
      {:ok, :completed} -> 
      {:error, reason} -> {:error, reason, state}
    end
  end
  
  defp fetch_individual_properties(conn, realm, file_descriptor, state, mapping, device_id, interface_id, opts) do
    with {:more_data, data, updated_options} <- FetchData.fetch_individual_properties(conn, realm, mappings, device_id, interface_id, options) do

    else
      {:ok, :completed} -> 
      {:error, reason} -> {:error, reason, state}
    end
  end
end
