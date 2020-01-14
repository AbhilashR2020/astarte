# astarte_export

Astarte Export is an easy to use tool that allows to exporting all the devices and data from an existing Astarte realm to XML format.





```iex
ex(astarte_export@127.0.0.1)2> Astarte.Export.export_realm_data("test", "/home/harika/MyApplication/final_package/astarte_export-master/_build/dev/rel/astarte_export")
8:45:23.131     |INFO | Export started.                                         | module=Elixir.Astarte.Export function=generate_xml/2 realm=test
8:45:23.146     |INFO | Connected to database.                                  | module=Elixir.Astarte.Export function=get_value/2 realm=test 
8:45:23.236     |INFO | Extracted devices information from realm                | module=Elixir.Astarte.Export function=get_value/2 realm=test 
8:45:23.489     |INFO | XML Seralization completed                              | module=Elixir.Astarte.Export function=generate_xml/2 realm=test
8:45:23.490     |INFO | Export completed into file: /home/harika/MyApplication/final_package/astarte_export-master/_build/dev/rel/astarte_export/test_2019_12_30_8_45_23.xml    | module=Elixir.Astarte.Export function=generate_xml/2 realm=test
:ok
iex(astarte_export@127.0.0.1)3>
```

```xml
<astarte>
<devices>
<device device_id="yKA3CMd07kWaDyj6aMP4Dg">
  <protocol pending_empty_cache="false" revision="0"></protocol>
  <registration first_registration="2019-05-30T13:49:57.045Z" secret_bcrypt_hash="$2b$12$bKly9EEKmxfVyDeXjXu1vOebWgr34C8r4IHd9Cd.34Ozm0TWVo1Ve"></registration>
  <credentials cert_aki="a8eaf08a797f0b10bb9e7b5dca027ec2571c5ea6" cert_serial="324725654494785828109237459525026742139358888604" first_credentials_request="2019-05-30T13:49:57.355Z" inhibit_request="false"></credentials>
  <stats last_connection="2019-05-30T13:49:57.561Z" last_disconnection="2019-05-30T13:51:00.038Z" last_seen_ip="198.51.100.89" total_received_bytes="3960" total_received_msgs="64"></stats>
  <interfaces>
    <interface active="true" major_version="0" minor_version="1" name="testinterfaceobject.org">
      <datastream>
        <object reception_timestamp="2019-06-11T13:26:44.218Z">
          <item name="/y">20.0</item>
          <item name="/z">30.0</item>
        </object>
        <object reception_timestamp="2019-06-11T13:26:28.994Z">
          <item name="/x">1.0</item>
          <item name="/z">3.0</item>
        </object>
        <object reception_timestamp="2019-06-11T13:24:03.200Z">
          <item name="/x">0.1</item>
          <item name="/y">0.2</item>
        </object>
      </datastream>
    </interface>
    <interface active="true" major_version="0" minor_version="1" name="testinterface.org">
      <datastream>
        <value reception_timestamp="2019-05-31T09:12:42.789Z">74847848744474874</value>
        <value reception_timestamp="2019-05-31T09:13:29.144Z">78787484848484873</value>
        <value reception_timestamp="2019-05-31T09:13:52.040Z">87364787847847847</value>
      </datastream>
      <datastream>
        <value reception_timestamp="2019-05-31T09:12:42.789Z">2019-05-31T10:12:42.000Z</value>
      </datastream>
      <datastream>
        <value reception_timestamp="2019-05-31T09:12:42.789Z">true</value>
        <value reception_timestamp="2019-05-31T09:13:29.144Z">true</value>
        <value reception_timestamp="2019-05-31T09:13:52.040Z">true</value>
        <value reception_timestamp="2019-05-31T09:25:42.789Z">true</value>
      </datastream>
      <datastream>
        <value reception_timestamp="2019-05-31T09:12:42.789Z">1</value>
        <value reception_timestamp="2019-05-31T09:13:29.144Z">2</value>
        <value reception_timestamp="2019-05-31T09:13:42.789Z">1</value>
        <value reception_timestamp="2019-05-31T09:13:52.040Z">3</value>
        <value reception_timestamp="2019-05-31T09:14:29.144Z">2</value>
        <value reception_timestamp="2019-05-31T09:15:52.040Z">3</value>
      </datastream>
      <datastream>
        <value reception_timestamp="2019-05-31T09:12:42.789Z">0.1</value>
        <value reception_timestamp="2019-05-31T09:13:29.144Z">0.2</value>
        <value reception_timestamp="2019-05-31T09:13:52.040Z">0.3</value>
      </datastream>
      <datastream>
        <value reception_timestamp="2019-05-31T09:12:42.789Z">This is my string</value>
        <value reception_timestamp="2019-05-31T09:13:29.144Z">This is my string2</value>
        <value reception_timestamp="2019-05-31T09:13:52.040Z">This is my string3</value>
      </datastream>
    </interface>
    <interface active="true" major_version="0" minor_version="1" name="com.example.properties">
      <property path="/properties1" reception_timestamp="2020-01-06T00:44:26.921Z">4.2</property>
    </interface>
  </interfaces>
</device>
</devices>
</astarte>
```


