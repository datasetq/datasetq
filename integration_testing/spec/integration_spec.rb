require 'json'
require 'minitest/autorun'
require 'tempfile'

class TestDsqCli < Minitest::Test
  def setup
    @dsq_path = '../target/release/dsq' # Adjust path as needed
    @input_json = 'test_data.json'
    @output_parquet = 'output.parquet'
    @back_to_json = 'back_to_json.json'

    # Create test JSON data
    test_data = [
      { name: 'Alice', age: 30, city: 'New York' },
      { name: 'Bob', age: 25, city: 'San Francisco' },
      { name: 'Charlie', age: 35, city: 'Chicago' }
    ]
    File.write(@input_json, test_data.to_json)
  end

  def teardown
    File.delete(@input_json) if File.exist?(@input_json)
    File.delete(@output_parquet) if File.exist?(@output_parquet)
    File.delete(@back_to_json) if File.exist?(@back_to_json)
  end

  def test_json_to_parquet_conversion
    # Convert JSON to Parquet
    system("#{@dsq_path} '.' #{@input_json} --output #{@output_parquet}")
    assert File.exist?(@output_parquet)

    # Convert Parquet back to JSON
    system("#{@dsq_path} '.' #{@output_parquet} --output #{@back_to_json}")
    assert File.exist?(@back_to_json)

    # Verify the data is the same
    original_data = JSON.parse(File.read(@input_json))
    converted_data = JSON.parse(File.read(@back_to_json))
    assert_equal original_data, converted_data
  end

  def test_csv_to_parquet_conversion
    # Create CSV test data
    csv_data = "name,age,city\nAlice,30,New York\nBob,25,San Francisco\nCharlie,35,Chicago\n"
    File.write('test_data.csv', csv_data)

    # Convert CSV to Parquet
    system("#{@dsq_path} '.' test_data.csv --output test_output.parquet")
    assert File.exist?('test_output.parquet')

    # Convert Parquet back to JSON for verification
    system("#{@dsq_path} '.' test_output.parquet --output test_back_to_json.json")
    assert File.exist?('test_back_to_json.json')

    # Verify the data
    original_data = [
      {"name" => "Alice", "age" => 30, "city" => "New York"},
      {"name" => "Bob", "age" => 25, "city" => "San Francisco"},
      {"name" => "Charlie", "age" => 35, "city" => "Chicago"}
    ]
    converted_data = JSON.parse(File.read('test_back_to_json.json'))
    assert_equal original_data, converted_data

    # Clean up
    File.delete('test_data.csv') if File.exist?('test_data.csv')
    File.delete('test_output.parquet') if File.exist?('test_output.parquet')
    File.delete('test_back_to_json.json') if File.exist?('test_back_to_json.json')
  end

  def test_tsv_to_parquet_conversion
    # Create TSV test data
    tsv_data = "name\tage\tcity\nAlice\t30\tNew York\nBob\t25\tSan Francisco\nCharlie\t35\tChicago\n"
    File.write('test_data.tsv', tsv_data)

    # Convert TSV to Parquet
    system("#{@dsq_path} '.' test_data.tsv --output test_output.parquet")
    assert File.exist?('test_output.parquet')

    # Convert Parquet back to JSON for verification
    system("#{@dsq_path} '.' test_output.parquet --output test_back_to_json.json")
    assert File.exist?('test_back_to_json.json')

    # Verify the data
    original_data = [
      {"name" => "Alice", "age" => 30, "city" => "New York"},
      {"name" => "Bob", "age" => 25, "city" => "San Francisco"},
      {"name" => "Charlie", "age" => 35, "city" => "Chicago"}
    ]
    converted_data = JSON.parse(File.read('test_back_to_json.json'))
    assert_equal original_data, converted_data

    # Clean up
    File.delete('test_data.tsv') if File.exist?('test_data.tsv')
    File.delete('test_output.parquet') if File.exist?('test_output.parquet')
    File.delete('test_back_to_json.json') if File.exist?('test_back_to_json.json')
  end

  def test_jsonlines_to_parquet_conversion
    # Create JSON Lines test data
    jsonl_data = "{\"name\":\"Alice\",\"age\":30,\"city\":\"New York\"}\n{\"name\":\"Bob\",\"age\":25,\"city\":\"San Francisco\"}\n{\"name\":\"Charlie\",\"age\":35,\"city\":\"Chicago\"}\n"
    File.write('test_data.jsonl', jsonl_data)

    # Convert JSON Lines to Parquet
    system("#{@dsq_path} '.' test_data.jsonl --output test_output.parquet")
    assert File.exist?('test_output.parquet')

    # Convert Parquet back to JSON for verification
    system("#{@dsq_path} '.' test_output.parquet --output test_back_to_json.json")
    assert File.exist?('test_back_to_json.json')

    # Verify the data
    original_data = [
      {"name" => "Alice", "age" => 30, "city" => "New York"},
      {"name" => "Bob", "age" => 25, "city" => "San Francisco"},
      {"name" => "Charlie", "age" => 35, "city" => "Chicago"}
    ]
    converted_data = JSON.parse(File.read('test_back_to_json.json'))
    assert_equal original_data, converted_data

    # Clean up
    File.delete('test_data.jsonl') if File.exist?('test_data.jsonl')
    File.delete('test_output.parquet') if File.exist?('test_output.parquet')
    File.delete('test_back_to_json.json') if File.exist?('test_back_to_json.json')
  end

  def test_json5_to_parquet_conversion
    # Create JSON5 test data
    json5_data = "[
  {name: \"Alice\", age: 30, city: \"New York\"},
  {name: \"Bob\", age: 25, city: \"San Francisco\"},
  {name: \"Charlie\", age: 35, city: \"Chicago\"}
]"
    File.write('test_data.json5', json5_data)

    # Convert JSON5 to Parquet
    system("#{@dsq_path} '.' test_data.json5 --output test_output.parquet")
    assert File.exist?('test_output.parquet')

    # Convert Parquet back to JSON for verification
    system("#{@dsq_path} '.' test_output.parquet --output test_back_to_json.json")
    assert File.exist?('test_back_to_json.json')

    # Verify the data
    original_data = [
      {"name" => "Alice", "age" => 30, "city" => "New York"},
      {"name" => "Bob", "age" => 25, "city" => "San Francisco"},
      {"name" => "Charlie", "age" => 35, "city" => "Chicago"}
    ]
    converted_data = JSON.parse(File.read('test_back_to_json.json'))
    assert_equal original_data, converted_data

    # Clean up
    File.delete('test_data.json5') if File.exist?('test_data.json5')
    File.delete('test_output.parquet') if File.exist?('test_output.parquet')
    File.delete('test_back_to_json.json') if File.exist?('test_back_to_json.json')
  end

  def test_output_formatting_options
    # Test compact output
    output = `#{@dsq_path} -c '.' #{@input_json}`
    assert $?.success?
    assert !output.include?("\n  ") # Should not have indentation

    # Test raw output
    output = `#{@dsq_path} -r '.name' #{@input_json}`
    assert $?.success?
    assert_equal "Alice\nBob\nCharlie\n", output

    # Test sort keys
    output = `#{@dsq_path} -S '. | sort_by(.name)' #{@input_json}`
    assert $?.success?
    # Should work without error
  end

  def test_csv_options
    # Create CSV test data
    csv_data = "name,age,city\nAlice,30,New York\nBob,25,San Francisco\n"
    File.write('test_data.csv', csv_data)

    # Test CSV separator
    output = `#{@dsq_path} --csv-separator ',' '.' test_data.csv`
    assert $?.success?

    # Test CSV headers
    output = `#{@dsq_path} --csv-headers true '.' test_data.csv`
    assert $?.success?

    File.delete('test_data.csv') if File.exist?('test_data.csv')
  end

  def test_processing_options
    # Test limit
    output = `#{@dsq_path} --limit 1 '.' #{@input_json}`
    assert $?.success?
    result = JSON.parse(output)
    assert_equal 1, result.length

    # Test select columns
    output = `#{@dsq_path} --select name,age '.' #{@input_json}`
    assert $?.success?
    result = JSON.parse(output)
    result.each do |item|
      assert item.key?('name')
      assert item.key?('age')
      assert !item.key?('city')
    end
  end

  def test_performance_options
    # Test batch size
    output = `#{@dsq_path} --batch-size 10 '.' #{@input_json}`
    assert $?.success?

    # Test threads
    output = `#{@dsq_path} --threads 2 '.' #{@input_json}`
    assert $?.success?

    # Test parallel disabled
    output = `#{@dsq_path} --parallel false '.' #{@input_json}`
    assert $?.success?
  end

  def test_debug_options
    # Test verbose
    output = `#{@dsq_path} -v '.' #{@input_json}`
    assert $?.success?

    # Test quiet
    output = `#{@dsq_path} --quiet '.' #{@input_json}`
    assert $?.success?
    # Should have minimal output
  end

  def test_variable_options
    # Test --arg
    output = `#{@dsq_path} --arg test_var value 'map({name: .name, age: .age, city: .city, test: $test_var})' #{@input_json}`
    assert $?.success?
    result = JSON.parse(output)
    result.each do |item|
      assert_equal 'value', item['test']
    end

    # Test --argjson
    output = `#{@dsq_path} --argjson config '{"enabled": true}' 'map({name: .name, age: .age, city: .city, enabled: $config.enabled})' #{@input_json}`
    assert $?.success?
    result = JSON.parse(output)
    result.each do |item|
      assert item['enabled']
    end
  end

  def test_subcommands
    # Test convert subcommand
    system("#{@dsq_path} convert #{@input_json} test_output.csv --overwrite")
    assert File.exist?('test_output.csv')

    # Test inspect subcommand
    output = `#{@dsq_path} inspect #{@input_json}`
    assert $?.success?
    assert output.include?('Format')

    # Test validate subcommand
    output = `#{@dsq_path} validate #{@input_json}`
    assert $?.success?

    # Clean up
    File.delete('test_output.csv') if File.exist?('test_output.csv')
  end

  def test_config_subcommands
    # Test config show
    output = `#{@dsq_path} config show`
    assert $?.success?

    # Test config init
    system("#{@dsq_path} config init test_config.toml --force")
    assert File.exist?('test_config.toml')

    # Test config check
    output = `#{@dsq_path} config check test_config.toml`
    assert $?.success?

    # Clean up
    File.delete('test_config.toml') if File.exist?('test_config.toml')
  end

  def test_filter_file_option
    # Create a filter file
    File.write('test_filter.dsq', '.name')

    # Test using filter file
    output = `#{@dsq_path} -f test_filter.dsq #{@input_json}`
    assert $?.success?
    result = JSON.parse(output)
    assert_equal ['Alice', 'Bob', 'Charlie'], result

    File.delete('test_filter.dsq') if File.exist?('test_filter.dsq')
  end

  def test_field_access_with_spaces
    # Create test data with spaces in field names
    test_data_spaces = [
      { "US City Name" => "New York", "population" => 8500000, "country code" => "US" },
      { "US City Name" => "Los Angeles", "population" => 4000000, "country code" => "US" },
      { "US City Name" => "Chicago", "population" => 2700000, "country code" => "US" }
    ]
    File.write('test_spaces.json', test_data_spaces.to_json)

    # Test bracket notation for field access with spaces
    output = `#{@dsq_path} '.[\"US City Name\"]' test_spaces.json`
    assert $?.success?
    result = JSON.parse(output)
    expected = ["New York", "Los Angeles", "Chicago"]
    assert_equal expected, result

    # Test bracket notation for population field
    output = `#{@dsq_path} '.[\"population\"]' test_spaces.json`
    assert $?.success?
    result = JSON.parse(output)
    expected = [8500000, 4000000, 2700000]
    assert_equal expected, result

    # Test bracket notation for country code
    output = `#{@dsq_path} '.[\"country code\"]' test_spaces.json`
    assert $?.success?
    result = JSON.parse(output)
    expected = ["US", "US", "US"]
    assert_equal expected, result

    # Test mixed dot and bracket notation
    output = `#{@dsq_path} 'map({\"city\": .[\"US City Name\"], \"pop\": .[\"population\"]})' test_spaces.json`
    assert $?.success?
    result = JSON.parse(output)
    assert_equal "New York", result[0]["city"]
    assert_equal 8500000, result[0]["pop"]

    File.delete('test_spaces.json') if File.exist?('test_spaces.json')
  end

  def test_csv_field_access_with_spaces
    # Create CSV test data with spaces in column names
    csv_data = "\"US City Name\",population,\"country code\"\n\"New York\",8500000,US\n\"Los Angeles\",4000000,US\n\"Chicago\",2700000,US\n"
    File.write('test_spaces.csv', csv_data)

    # Test bracket notation for CSV field access with spaces
    output = `#{@dsq_path} '.[\"US City Name\"]' test_spaces.csv`
    assert $?.success?
    result = JSON.parse(output)
    expected = ["New York", "Los Angeles", "Chicago"]
    assert_equal expected, result

    # Test bracket notation for population
    output = `#{@dsq_path} '.[\"population\"]' test_spaces.csv`
    assert $?.success?
    result = JSON.parse(output)
    expected = [8500000, 4000000, 2700000]
    assert_equal expected, result

    File.delete('test_spaces.csv') if File.exist?('test_spaces.csv')
  end

  def test_parquet_field_access_with_spaces
    # Create test data with spaces in field names
    test_data_spaces = [
      { "US City Name" => "New York", "population" => 8500000, "country code" => "US" },
      { "US City Name" => "Los Angeles", "population" => 4000000, "country code" => "US" },
      { "US City Name" => "Chicago", "population" => 2700000, "country code" => "US" }
    ]
    File.write('test_spaces_parquet.json', test_data_spaces.to_json)

    # Convert JSON to Parquet
    system("#{@dsq_path} '.' test_spaces_parquet.json --output test_spaces.parquet")
    assert File.exist?('test_spaces.parquet')

    # Test bracket notation for Parquet field access with spaces
    output = `#{@dsq_path} '.[\"US City Name\"]' test_spaces.parquet`
    assert $?.success?
    result = JSON.parse(output)
    expected = ["New York", "Los Angeles", "Chicago"]
    assert_equal expected, result

    # Test bracket notation for population in Parquet
    output = `#{@dsq_path} '.[\"population\"]' test_spaces.parquet`
    assert $?.success?
    result = JSON.parse(output)
    expected = [8500000, 4000000, 2700000]
    assert_equal expected, result

    # Clean up
    File.delete('test_spaces_parquet.json') if File.exist?('test_spaces_parquet.json')
    File.delete('test_spaces.parquet') if File.exist?('test_spaces.parquet')
  end

  def test_error_handling
    # Test invalid filter
    output = `#{@dsq_path} '.invalid_syntax[[' #{@input_json} 2>&1`
    assert !$?.success?
    assert output.include?('Error') || output.include?('error')
  end

  def test_exit_status_option
    # Test exit status with true condition
    output = `#{@dsq_path} -e '.age > 20' #{@input_json}`
    assert $?.success?

    # Test exit status with false condition
    output = `#{@dsq_path} -e '.age > 50' #{@input_json}`
    assert !$?.success?
  end
end