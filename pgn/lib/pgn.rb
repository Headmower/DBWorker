require "pgn/board"
require "pgn/fen"
require "pgn/game"
require "pgn/move"
require "pgn/move_calculator"
require "pgn/parser"
require "pgn/position"
require "pgn/version"

module PGN

  # @param pgn [String] a pgn representation of one or more chess games
  # @return [Array<PGN::Game>] a list of games
  #
  # @note The PGN spec specifies Latin-1 as the encoding for PGN files, so
  #   this is default.
  #
  # @see http://www.chessclub.com/help/PGN-spec PGN Specification
  #
  def self.parse(pgn, encoding = Encoding::ISO_8859_1)
  	if pgn.is_a? String
      begin
        # Try it as UTF-8 directly
        cleaned = pgn.dup.force_encoding('UTF-8')
        unless cleaned.valid_encoding?
          # Some of it might be old Windows code page
          cleaned = pgn.encode( 'UTF-8', 'Windows-1252' )
        end
        pgn = cleaned
      rescue EncodingError
        # Force it to UTF-8, throwing out invalid bits
        pgn.encode!( 'UTF-8', invalid: :replace, undef: :replace, :replace=>"?" )
      end
      PGN::Parser.new.parse(pgn).map do |game|
          PGN::Game.new(game[:moves], game[:tags], game[:result])
      end
    end
  end


  def self.parse_big(filename,step = 900000,encoding = Encoding::ISO_8859_1)
    if(!filename || !File.exist?(filename))
      raise "Give me a path to a file for parsing..."
    end
    position = 0
    a = 0
    games = []
    start_time = Time.now
    while((file = File.read(filename, step, a.to_i)) != nil)
      if file.length<step
        file += "\n\n\["
      end
      start = nil
      start_next = nil
      b = /\r?\n\r?\n\[/.match(file)
    while !b.nil?
      start = b.begin(0)
      start_next = start + b[0].length - 1
      b = /\r?\n\r?\n\[/.match(file, start_next)
      end
    if start.nil?
      puts "End of file =("
      break
    end
      position = position + step
      print "\rParsed up to #{position} position"
      games += PGN.parse(file[0..start.to_i])
      a = a + start_next
    end
    return games
  end
end