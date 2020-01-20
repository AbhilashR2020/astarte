defmodule Astarte.Export.XMLGenerate do

  def xml_write(:default_header, fd) do
    {:ok, doc, state} = XMLStreamWriter.new_document()
    {:ok, header, state} = XMLStreamWriter.start_document(state) 
    xml_data = :erlang.iolist_to_binary([header])
    IO.puts(fd, xml_data)
    {:ok, state}
  end

  def xml_write(:empty_element, fd, {tag, attributes, []}, state) do
    {:ok, empty_tag, state} = XMLStreamWriter.empty_element(state, tag, attributes)
    xml_data = :erlang.iolist_to_binary([empty_tag])
    IO.puts(fd, xml_data) 
    {:ok, state}
  end
  
  def xml_write(:full_element, fd, {tag, attributes, value}, state) do
    {:ok, start_tag, state}  = XMLStreamWriter.start_element(state, tag, attributes)
    {:ok, data, state}  = XMLStreamWriter.characters(state, value) 
    {:ok, end_tag, state}  = XMLStreamWriter.end_element(state) 
    xml_data = :erlang.iolist_to_binary([start_tag, data, end_tag])
    IO.puts(fd, xml_data)
    {:ok, state}
  end

  def xml_write(:start_tag, fd, {tag, attributes}, state) do
    {:ok, start_tag, state}  = XMLStreamWriter.start_element(state, tag, attributes)
    xml_data = :erlang.iolist_to_binary([start_tag])
    IO.puts(fd, xml_data)
    {:ok, state}
  end
 
  def xml_write(:end_tag, fd, state) do
    {:ok, end_tag, state}  = XMLStreamWriter.end_element(state)  
    xml_data = :erlang.iolist_to_binary([end_tag])
    IO.puts(fd, xml_data)
    {:ok, state} 
  end
  
end  
      
