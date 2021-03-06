require 'whittle'

module PGN
  # {PGN::Parser} uses the whittle gem to parse pgn files based on their
  # context free grammar.

  class Parser < Whittle::Parser

    def lex(input)
      line   = 1
      offset = 0
      ending = input.length

      until offset == ending do
        next_token(input, offset, line).tap do |token|
          if !token.nil?
            token[:offset] = offset
            line, token[:line] = token[:line], line
            yield token unless token[:discarded]
            offset += token[:value].length
          else
            raise UnconsumedInputError,
            "Unmatched input #{input[offset..-1].inspect} on line #{line}"
            #offset += 1
          end
        end
      end

      yield ({ :name => :$end, :line => line, :value => nil, :offset => offset })
    end

    rule(:wsp => /\s+/).skip!

    rule("[")
    rule("]")
    rule("(")
    rule(")")

    start(:pgn_database)

    rule(:pgn_database) do |r|
      r[].as { [] }
      r[:pgn_game, :pgn_database].as {|game, database| database << game }
    end

    rule(:pgn_game) do |r|
      r[:tag_section, :movetext_section].as {|tags, moves| {tags: tags, result: moves.pop, moves: moves} }
    end

    rule(:tag_section) do |r|
      r[:tag_pair, :tag_section].as {|pair, section| section.merge(pair) }
      r[:tag_pair]
    end

    rule(:tag_pair) do |r|
      r["[", :tag_name, :tag_value, "]"].as {|_, a, b, _| {a => b} }
    end

    rule(:tag_value) do |r|
      r[:string].as {|value| value[1...-1] }
    end

    rule(:movetext_section) do |r|
      r[:element_sequence, :game_termination].as {|a, b| a.reverse << b }
    end

    rule(:element_sequence) do |r|
      r[:element, :element_sequence].as {|element, sequence| element.nil? ? sequence : sequence << element }
      r[].as { [] }
      r[:comment, :element_sequence].as { |_, sequence| sequence }
      r[:line_comment, :element_sequence].as { |_, sequence| sequence }
      r[:recursive_variation, :element_sequence].as {|_, sequence| sequence}
      #r[:recursive_variation]
    end

    rule(:element) do |r|
      r[:move_number_indication].as { nil }
      r[:san_move]
      r[:numeric_annotation_glyph].as { nil }
    end

    rule(
      :comment => %r{
        \{
          [^\}]*
        \}
      }x
    )

    rule(
      :line_comment => %r{
        ;
        .*
        $
      }x
    )
=begin
    rule(
      :string => %r{
        "                          # beginning of string
        (
          [[:print:]&&[^\\"]] |    # printing characters except quote and backslash
          \\\\                |    # escaped backslashes
          \\"                      # escaped quotation marks
        )*                         # zero or more of the above
        "                          # end of string
      }x
    )
=end
#=begin
    rule(
      :string => %r{
        "                          # beginning of string
         .*                        # printing characters except quote and backslash
        "                          # end of string
      }x
    )
#=end
    rule(
      :game_termination => %r{
        1-0       |    # white wins
        0-1       |    # black wins
        1\/2-1\/2 |    # draw
        \*             # ?
      }x
    )

    rule(
      :move_number_indication => %r{
        [[:digit:]]+\.*    # one or more digits followed by zero or more periods
      }x
    )

    rule(
      :san_move => %r{
        (
          [O0](-[O0]){1,2}             |    # castling (O-O, O-O-O)
          [a-h][1-8]                   |    # pawn moves (e4, d7)
          [BKNQR][a-h1-8]?x?[a-h][1-8] |    # major piece moves w/ optional specifier
                                            # and capture
                                            # (Bd2, N4c3, Raxc1)
          [a-h][1-8]?x[a-h][1-8]            # pawn captures
        )
        (
          =[BNQR]                            # optional promotion (d8=Q)
        )?
        (
          \+                            |    # check (g5+)
          \#                                 # checkmate (Qe7#)
        )?
      }x
    )

    rule(
      :tag_name => %r{
        [A-Za-z0-9_]+    # letters, digits and underscores only
      }x
    )

    rule(
      :numeric_annotation_glyph => %r{
        \$\d+    # dollar sign followed by an integer from 0 to 255
      }x
    )

    rule(:recursive_variation) do |r|
      r["(", :element_sequence, ")"]
    end
  end
end
