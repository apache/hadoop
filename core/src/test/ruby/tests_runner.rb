require 'rubygems'
require 'rake'

require 'test_helper'

puts "Running tests..."

files = Dir[ File.dirname(__FILE__) + "/**/*_test.rb" ]
files.each do |file|
  begin
    load(file)
  rescue => e
    puts "ERROR: #{e}"
    raise
  end
end

puts "Done with tests!"
