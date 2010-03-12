require 'shell'
require 'shell/formatter'

class ShellCommandsTest < Test::Unit::TestCase
  Shell.commands.each do |name, klass|
    define_test "#{name} command class #{klass} should respond to help" do
      assert_respond_to(klass.new(nil), :help)
    end

    define_test "#{name} command class #{klass} should respond to :command" do
      assert_respond_to(klass.new(nil), :command)
    end
  end
end
