require 'test/unit'

module Testing
  module Declarative
    # define_test "should do something" do
    #   ...
    # end
    def define_test(name, &block)
      test_name = "test_#{name.gsub(/\s+/,'_')}".to_sym
      defined = instance_method(test_name) rescue false
      raise "#{test_name} is already defined in #{self}" if defined
      if block_given?
        define_method(test_name, &block)
      else
        define_method(test_name) do
          flunk "No implementation provided for #{name}"
        end
      end
    end
  end
end

module Hbase
  module TestHelpers
    def setup_hbase
      @formatter = Shell::Formatter::Console.new()
      @hbase = ::Hbase::Hbase.new($TEST_CLUSTER.getConfiguration)
    end

    def table(table)
      @hbase.table(table, @formatter)
    end

    def admin
      @hbase.admin(@formatter)
    end

    def create_test_table(name)
      # Create the table if needed
      unless admin.exists?(name)
        admin.create name, [{'NAME' => 'x', 'VERSIONS' => 5}, 'y']
        return
      end

      # Enable the table if needed
      unless admin.enabled?(name)
        admin.enable(name)
      end
    end

    def drop_test_table(name)
      return unless admin.exists?(name)
      begin
        admin.disable(name) if admin.enabled?(name)
      rescue => e
        puts "IGNORING DISABLE TABLE ERROR: #{e}"
      end
      begin
        admin.drop(name)
      rescue => e
        puts "IGNORING DROP TABLE ERROR: #{e}"
      end
    end
  end
end

# Extend standard unit tests with our helpers
Test::Unit::TestCase.extend(Testing::Declarative)

# Add the $HBASE_HOME/lib/ruby directory to the ruby
# load path so I can load up my HBase ruby modules
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "..", "main", "ruby")
