# Results formatter
module Formatter
  # Base abstract class for results formatting.
  class Formatter
    def is_kernel?(obj)
      obj.kind_of?(Module) and obj.name == "Kernel"
    end

    # Takes an output stream and a print width.
    def initialize(opts={})
      defaults = {:output_stream => Kernel, :format_width => 100}
      options = defaults.merge(opts)

      @out = options[:output_stream]
      raise TypeError.new("Type %s of parameter %s is not IO" % [@out.class, @out]) \
        unless @out.instance_of? IO or is_kernel?(@out)

      @maxWidth = options[:format_width]
      @rowCount = 0
    end

    attr_reader :rowCount

    def header(args = [], widths = [])
      row(args, false, widths) if args.length > 0
      @rowCount = 0
    end
    
    # Output a row.
    # Inset is whether or not to offset row by a space.
    def row(args = [], inset = true, widths = [])
      if not args or args.length == 0
        # Print out nothing
        return
      end
      if args.class == String
        output(@maxWidth, args)
        @out.puts
        return
      end
      # TODO: Look at the type.  Is it RowResult?
      if args.length == 1
        splits = split(@maxWidth, dump(args[0]))
        for l in splits
          output(@maxWidth, l)
          @out.puts
        end
      elsif args.length == 2
        col1width = (not widths or widths.length == 0) ? @maxWidth / 4 : @maxWidth * widths[0] / 100
        col2width = (not widths or widths.length < 2) ? @maxWidth - col1width - 2 : @maxWidth * widths[1] / 100 - 2
        splits1 = split(col1width, dump(args[0]))
        splits2 = split(col2width, dump(args[1]))
        biggest = (splits2.length > splits1.length)? splits2.length: splits1.length
        index = 0
        while index < biggest
          if inset
            # Inset by one space if inset is set.
            @out.print(" ")
          end
          output(col1width, splits1[index])
          if not inset
            # Add extra space so second column lines up w/ second column output
            @out.print(" ")
          end
          @out.print(" ")
          output(col2width, splits2[index])
          index += 1
          @out.puts
        end
      else
        # Print a space to set off multi-column rows
        print ' '
        first = true
        for e in args
          @out.print " " unless first
          first = false
          @out.print e
        end
        puts
      end
      @rowCount += 1
    end

    def split(width, str)
      result = []
      index = 0
      while index < str.length do
        result << str.slice(index, width)
        index += width
      end
      result
    end

    def dump(str)
      if str.instance_of? Fixnum
          return
      end
      # Remove double-quotes added by 'dump'.
      return str.dump[1..-2]
    end

    def output(width, str)
      # Make up a spec for printf
      spec = "%%-%ds" % width
      @out.printf(spec, str)
    end

    def footer(startTime = nil, rowCount = nil)
      if not rowCount
        rowCount = @rowCount
      end
      if not startTime
        return
      end
      # Only output elapsed time and row count if startTime passed
      @out.puts("%d row(s) in %.4f seconds" % [rowCount, Time.now - startTime])
    end
  end
     

  class Console < Formatter
  end

  class XHTMLFormatter < Formatter
    # http://www.germane-software.com/software/rexml/doc/classes/REXML/Document.html
    # http://www.crummy.com/writing/RubyCookbook/test_results/75942.html
  end

  class JSON < Formatter
  end

  # Do a bit of testing.
  if $0 == __FILE__
    formatter = Console.new(STDOUT)
    now = Time.now
    formatter.header(['a', 'b'])
    formatter.row(['a', 'b'])
    formatter.row(['xxxxxxxxx xxxxxxxxxxx xxxxxxxxxxx xxxxxxxxxxxx xxxxxxxxx xxxxxxxxxxxx xxxxxxxxxxxxxxx xxxxxxxxx xxxxxxxxxxxxxx'])
    formatter.row(['yyyyyy yyyyyy yyyyy yyy', 'xxxxxxxxx xxxxxxxxxxx xxxxxxxxxxx xxxxxxxxxxxx xxxxxxxxx xxxxxxxxxxxx xxxxxxxxxxxxxxx xxxxxxxxx xxxxxxxxxxxxxx  xxx xx x xx xxx xx xx xx x xx x x xxx x x xxx x x xx x x x x x x xx '])
    formatter.row(["NAME => 'table1', FAMILIES => [{NAME => 'fam2', VERSIONS => 3, COMPRESSION => 'NONE', IN_MEMORY => false, BLOCKCACHE => false, LENGTH => 2147483647, TTL => FOREVER, BLOOMFILTER => NONE}, {NAME => 'fam1', VERSIONS => 3, COMPRESSION => 'NONE', IN_MEMORY => false, BLOCKCACHE => false, LENGTH => 2147483647, TTL => FOREVER, BLOOMFILTER => NONE}]"])
    formatter.footer(now)
  end
end


