 require 'pathname'

def count_lines(content)
  lines = content.lines
  test_lines = 0
  regular_lines = 0
  in_test = false
  brace_level = 0
  lines.each do |line|
    if line =~ /#\[test\]/ || line =~ /mod tests/
      in_test = true
      test_lines += 1
      brace_level += line.count('{')
      brace_level -= line.count('}')
      if brace_level == 0
        in_test = false
      end
    elsif in_test
      test_lines += 1
      brace_level += line.count('{')
      brace_level -= line.count('}')
      if brace_level == 0
        in_test = false
      end
    else
      regular_lines += 1
    end
  end
  [regular_lines, test_lines]
end

# Find all .rs files
rs_files = Dir.glob('src/**/*.rs')

# Group by crate
crates = {}

rs_files.each do |file|
  path = Pathname.new(file)
  parts = path.each_filename.to_a
  if parts[0] == 'src' && parts[1]
    crate = parts[1]
    crates[crate] ||= { files: [], total_lines: 0, total_regular_lines: 0, total_test_lines: 0, total_chars: 0 }
    content = File.read(file)
    regular_lines, test_lines = count_lines(content)
    total_lines = regular_lines + test_lines
    chars = content.size
    crates[crate][:files] << { path: file, regular_lines: regular_lines, test_lines: test_lines, total_lines: total_lines, chars: chars }
    crates[crate][:total_lines] += total_lines
    crates[crate][:total_regular_lines] += regular_lines
    crates[crate][:total_test_lines] += test_lines
    crates[crate][:total_chars] += chars
  end
end

# Output stats per crate
crates.each do |crate, data|
  num_files = data[:files].size
  avg_lines = num_files > 0 ? (data[:total_lines] / num_files.to_f).round(2) : 0
  puts "#{crate}:"
  puts "  Files: #{num_files}"
  puts "  Total lines: #{data[:total_lines]} (regular: #{data[:total_regular_lines]}, test: #{data[:total_test_lines]})"
  puts "  Total chars: #{data[:total_chars]}"
  puts "  Avg lines per file: #{avg_lines}"
  puts ""
end

# Top 10 files by regular lines
all_files = crates.values.flat_map { |data| data[:files] }
top_10 = all_files.sort_by { |f| -f[:regular_lines] }.first(10)

puts "Top 10 files by regular lines:"
top_10.each do |f|
  puts "#{f[:path]}: #{f[:regular_lines]} regular lines (test: #{f[:test_lines]})"
end