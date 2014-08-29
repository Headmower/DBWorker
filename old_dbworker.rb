#!/usr/bin/ruby
#encoding utf-8
$LOAD_PATH << '../../pgn/lib'

require 'rubygems'
require 'active_record'
require 'yaml'
require 'pgn'

load 'binary_search_tree.rb'
load 'binary_search_tree_hash.rb'

=begin
require 'inline'
class Array
	inline do |builder|
		builder.c_raw "
		  static VALUE average(int argc, VALUE *argv, VALUE self) {
		    double result = 0;
		    long  i, len;
		    VALUE *arr = RARRAY_PTR(self);
		    len = RARRAY_LEN(self);

		    for(i=0; i<len; i++) {
		      result += NUM2DBL(arr[i]);
		    }

		    return rb_float_new(result/(double)len);
		  }

		  static VALUE fastfind(){
		  	return (NULL);
		  }
		"
	end

	def self.middlepush(e)

	end
end
=end

class DBWorker

	@length = 0
	@games_count = 1
	def self.initialize()
		@@game_next_id = 1
		@@tag_next_id =  1
		@@pos_next_id =  1
		@@move_next_id = 1
		@@result = 0

		Dir["model/*.rb"].each { |file| load(file) }
		Dir["db/migrate/*.rb"].each { |file| load(file) }
		config = File.open('config/database.yml')
		dbconfig = YAML::load(config)
		config.close()
		#File.truncate(dbconfig["database"],0)
		ActiveRecord::Base.logger = nil # Logger.new(STDOUT)
		ActiveRecord::Base.establish_connection(dbconfig.merge('database' => 'postgres'))

		begin
			terminate_sql = <<-eos
				SELECT pg_terminate_backend(pg_stat_activity.pid)
				FROM   pg_stat_activity
				WHERE  pg_stat_activity.datname = 'gamebase'
		  			   AND pid <> pg_backend_pid();
		  	eos
		  	ActiveRecord::Base.connection.execute terminate_sql
		  	puts "Disconnected others"
		rescue Exception => e
			puts "Failed to disconnect due to: #{e}"
		end

		puts "Dropping DB #{dbconfig['database']}\n"
		ActiveRecord::Base.connection.drop_database dbconfig['database']
		puts "Creating DB #{dbconfig['database']}\n"
		ActiveRecord::Base.connection.create_database(dbconfig['database'])
		ActiveRecord::Base.establish_connection(dbconfig)
		# ActiveRecord::Migration.verbose = true
		ActiveRecord::Migrator.migrate 'db/migrate', nil
		ActiveRecord::Base.logger = nil
	end

	def self.parse(filename)

		DBWorker::initialize()
		GC.disable

		@@game_next_id = Game.last.try(:id) || 1
		@@tag_next_id =  Tag.last.try(:id) || 1
		@@pos_next_id =  Position.last.try(:id) || 1
		@@move_next_id = Move.last.try(:id) || 1
		@@result = 0
		@@shitty_games_count = 0

		step = 800000
		position = 0
	    start_time = Time.now
		while(file = File.read(filename, step, position))
			if(file.length<step)
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
			GC.disable
			DBWorker::parsefile(filename,file[0..start.to_i],position)
			position = position + start_next
			GC.enable
			GC.start
		end
		print "\n"
		end_time = Time.now
		duration = end_time - start_time
		puts "Total time: #{duration}"  
		puts "\nNumber of shitty games: #{@@shitty_games_count}"
		#puts "Time per game: #{duration/@length}"

	end

	def self.parsefile(filename,fil,position)

		GC.enable
		GC.start
		GC.disable

		shitty_games_file = File.open("shitty_games.shit","a")
		step = 200000
		a = 0
	    games = []
	    lengths = [position]
		while((file = fil[a.to_i..a.to_i+step]) != nil)
			if file.length<step
				file += "\n\n\["
			end
			start = nil
			start_next = 0
			b = /\r?\n\r?\n\[/.match(file,0)
			start = b[0].length - 1
			b = /\r?\n\r?\n\[/.match(file,start)
			nwgame = nil

			while !b.nil?
				begin
					nwgame = PGN.parse(file[start_next..b.begin(0)])[0]
					nwgame.fen_list
				rescue Exception => e
					puts e.inspect
					nwgame = nil
					shitty_games_file.write("#{file[start_next..b.begin(0)]}\r\r")
					@@shitty_games_count = @@shitty_games_count + 1
					print "\n.................SHIT MAFU..............\n#{file[start_next..b.begin(0)]}........................................\n"
					print "\r!!!!Shitty game detected on [#{start_next};#{b.begin(0)}] position!!!!\n"
				end
				start = b.begin(0)
				start_next = start + b[0].length - 1
				lengths << position+start_next + a.to_i - 1
				b = /\r?\n\r?\n\[/.match(file, start_next)
				games << nwgame
			end
			if start.nil?
				puts "End of file =("
				break
			end
			# nwgames = PGN.parse(file[0..start.to_i])
			GC.enable
			GC.start
			GC.disable
			a = a + start_next
			print "\rParsed up to #{lengths.last} position"
		end
		shitty_games_file.close
		print "\nParsing complete"
		file = nil
		GC.enable
		GC.start
		GC.disable
		DBWorker::fastparse(filename,games,lengths)
		#@length = @length + lengths.count
		return games.count.to_i
	end



	def self.fastparse(filename, games_raw,lengths)

		GC.enable
		GC.start
		GC.disable

		gamefile = GameFile.create!({file_path: filename})
		
		base_time = Time.now

		all_tags = BinarySearchTreeHash.new()
		Tag.all.select('id,name').each do |x|
			all_tags[x.name] = x
		end
		new_tags = BinarySearchTreeHash.new()

		# all_pos = BinarySearchTreeHash.new()
		# Position.all.select('id,fen_hash').each do |x|
		# 	all_pos[x.fen_hash] = x
		# end
		# new_pos = BinarySearchTreeHash.new()
		# all_moves = BinarySearchTreeHash.new()
		# Move.all.select('id,from_id,to_id').each do |x|
		# 	all_moves["#{x.from_id};#{x.to_id}"] = x
		# end
		# new_moves = BinarySearchTreeHash.new()

		new_games = []
		new_game_tags = []
		new_game_moves = []

		positions_file = File.open("positions.new", "w")
		moves_file = File.open("moves.new", "w")
		game_moves_file = File.open("game_moves.new", "w")
		#games_file = File.open('games.new', 'w')
		#tags_file = File.open('tags.new', "w")
		#game_tags_file = File.open('game_tags.new','w')

		games_raw.each_with_index do |gam,ind|
			if gam
				game = Game.new({id:@@game_next_id, game_file_id: gamefile.id, offset: lengths[ind],length: lengths[ind+1]-lengths[ind]})
				if gam.result == '0-1'
					game.result = -1
				elsif gam.result == '1-0'
					game.result = 1
				else game.result = 0
				end
				new_games << game
				#games_file.write("#{@@game_next_id}\t#{gamefile.id}\t#{lengths[ind]}\t#{lengths[ind+1]-lengths[ind]}\t#{@@result}\r")

				#ActiveRecord::Base.connection.execute "INSERT INTO 'games' VALUES (#{game.game_file_id},#{game.offset},#{game.length},#{game.result})"
				gam.tags.each do |tg|
	#=begin
					if !(tag = all_tags[tg[0]] || new_tags[tg[0]])
						tag = Tag.new({id:@@tag_next_id,name:tg[0]})
						@@tag_next_id = @@tag_next_id + 1
						new_tags[tag.name] = tag
					end
					new_game_tags << {tag_id: tag.id, game_id: game.id, tag_value: tg[1]}
	#=end
					#tag = Tag.where({name: tg[0]}).first || Tag.create!({name: tg[0]})
					#tags_file.write("#{@@tag_next_id}\t#{}\r")
					#@@tag_next_id = @@tag_next_id + 1
				end

				fen_list = gam.fen_list.to_a
				@@game_next_id = @@game_next_id + 1

				fenhash = 0
				#fenhash = fen_list[0].hash
				#positions_to_add = []
				#position1 = Position.where({fen_hash: fenhash}).first || Position.create!({fen: gam.fen_list[0], fen_hash: fenhash})
				# if !(position1 = all_pos[fenhash] || new_pos[fenhash])
					#position1 = Position.new({id: @@pos_next_id, fen: fen_list[0], fen_hash: fenhash})
					@@pos_next_id = @@pos_next_id + 1
					# new_pos[fenhash] = position1
					positions_file.write("#{@@pos_next_id}\t#{fen_list[0]}\t0\r")
				# end
				#if position1.nil?
				#	positions_to_add << gam.fen_list[0]
				#end
				position_1_id = @@pos_next_id
				
				#game_moves = []
				fen_list.each_with_index do |fn,i|
					if i>0
						position2 = nil
						#fenhash = fn.hash
						# if !(position2 = all_pos[fenhash] || new_pos[fenhash])
							# position2 = Position.new({id: @@pos_next_id, fen: fn, fen_hash: fenhash})
							@@pos_next_id = @@pos_next_id + 1
							# new_pos[fenhash] = position2
							positions_file.write("#{@@pos_next_id}\t#{fn}\t0\r")
							# positions_file.write("#{position2[:id]}\t#{position2[:fen]}\t#{position2[:fen_hash]}\r")
						# end
						#position2 = Position.create!({fen: fn})
						#move = Move.where({from_id: position1.id, to_id: position2.id}).first || Move.create!({text: gam.moves[i-1], from: position1, to: position2})
						# if !(move = all_moves["#{position1.id};#{position2.id}"] || new_moves["#{position1.id};#{position2.id}"])
							# move = Move.new({id: @@move_next_id, from_id: position1.id, to_id: position2.id, text: gam.moves[i-1]})
							@@move_next_id = @@move_next_id + 1
							# new_moves["#{position1.id};#{position2.id}"] = move
							moves_file.write("#{@@move_next_id}\t#{position_1_id}\t#{@@pos_next_id}\t#{gam.moves[i-1]}\r")
						# end
						#move = Move.create!({text: gam.moves[i-1], from: position1, to: position2})
						position_1_id = @@pos_next_id
						game_moves_file.write("#{@@move_next_id}\t#{game.id}\t#{i}\r")
						# new_game_moves << {move_id: @@move_next_id, game_id: game.id, half_move_number: i}
						#GameMove.create!({move_id: move.id, game_id: game.id, half_move_number: i})
					end
				end
			end
			print "\rProcessing Game #{@games_count+ind}"\
		end
		@games_count = @games_count + new_games.count+1
		print "\n"
		end_process_time = Time.now
		process_time = Time.now-base_time
		puts "Processed #{new_games.count} games in #{process_time} seconds"
		puts "Time per game: #{(process_time)/games_raw.count}"
		puts 'Writing games to file'
		#games_file.close
		File.write("games.new", new_games.map { |x| "#{x[:id]}\t#{x[:game_file_id]}\t#{x[:offset]}\t#{x[:length]}\t#{x[:result]}\r" }.join)
		puts 'Writing tags to file'
		#tags_file.close
		File.write("tags.new", new_tags.values.map { |x| "#{x[:id]}\t#{x[:name]}\r" }.join)
		puts 'Writing game tags to file'
		File.write("game_tags.new", new_game_tags.map { |x| "#{x[:game_id]}\t#{x[:tag_id]}\t#{x[:tag_value]}\r" }.join)
		puts 'Writing positions to file'
		positions_file.close
		#  do |file| 
		# 	new_pos.values.each { |pos| file.write("#{pos[:id]}\t#{pos[:fen]}\t#{pos[:fen_hash]}\r") }
		# end
		puts 'Writing moves to file'
		moves_file.close
		# File.write("moves.new", new_moves.values.map { |x| "#{x[:id]}\t#{x[:from_id]}\t#{x[:to_id]}\t#{x[:text]}\r" }.join)
		puts 'Writing game moves to file'
		game_moves_file.close
		# File.write("game_moves.new", new_game_moves.map { |x| "#{x[:move_id]}\t#{x[:game_id]}\t#{x[:half_move_number]}\r"}.join)
		puts 'Mass insert of games to DB'
		ActiveRecord::Base.connection.execute "COPY games (id, game_file_id, \"offset\", length, result) FROM 'C:\\Project.GameDB\\DBWorker\\games.new'"
		puts 'Mass insert of games to DB'
		ActiveRecord::Base.connection.execute "COPY tags (id, name) FROM 'C:\\Project.GameDB\\DBWorker\\tags.new'"
		puts 'Mass insert of tags to DB'
		ActiveRecord::Base.connection.execute "COPY game_tags (game_id, tag_id, tag_value) FROM 'C:\\Project.GameDB\\DBWorker\\game_tags.new'"
		puts 'Mass insert of game tags to DB'
		ActiveRecord::Base.connection.execute "COPY positions (id, fen, fen_hash) FROM 'C:\\Project.GameDB\\DBWorker\\positions.new'"
		puts 'Mass insert of positions to DB'
		ActiveRecord::Base.connection.execute "COPY moves (id, from_id, to_id,text) FROM 'C:\\Project.GameDB\\DBWorker\\moves.new'"
		puts 'Mass insert of game moves to DB'
		ActiveRecord::Base.connection.execute "COPY game_moves (move_id, game_id, half_move_number) FROM 'C:\\Project.GameDB\\DBWorker\\game_moves.new'"

		insert_time = Time.now-end_process_time
		puts "Loaded to DB in #{insert_time}\n"
		puts "Time per game: #{(insert_time)/games_raw.count}"
		return insert_time
	end

	def self.WriteParseRes(games)
		File.write("/parse_res/games.parsed", games)
	end

=begin
	def self.parse(filename, games,lengths)
		parse_time = Time.now-start_time
		base_time = Time.now
		games.each_with_index do |gam,ind|
			#game = Game.new({game_file_id: gamefile.id, offset: lengths[ind],length: lengths[ind+1]-lengths[ind]})
			#if gam.result == '0-1'
			#	game.result = -1
			#elsif gam.result == '1-0'
			#	game.result = 1
			#else game.result = 0
			#end
			#game.save!

			#ActiveRecord::Base.connection.execute "INSERT INTO 'games' VALUES (#{game.game_file_id},#{game.offset},#{game.length},#{game.result})"
			#gam.tags.each do |tg|
			#	# tghash = tg[0].hash
			#	tag = Tag.where({name: tg[0]}).first || Tag.create!({name: tg[0]})
			#	gametag = GameTag.create!({tag_id: tag.id, game_id: game.id, tag_value: tg[1]})
			#end
			fenhash = gam.fen_list[0].hash
			#positions_to_add = []
			position1 = Position.where({fen_hash: fenhash}).first || Position.create!({fen: gam.fen_list[0], fen_hash: fenhash})
			#if position1.nil?
			#	positions_to_add << gam.fen_list[0]
			#end
			
			#game_moves = []
			gam.fen_list.each_with_index do |fn,i|
				if i>0
					#position2 = nil
					fenhash = fn.hash
					position2 = Position.where({fen_hash: fenhash}).first || Position.create!({fen: fn,fen_hash: fenhash})
					#if position2.nil?
					#	positions_to_add << fn
					#end
					#position2 = Position.create!({fen: fn})
					#move = Move.where({from_id: position1.id, to_id: position2.id}).first || Move.create!({text: gam.moves[i-1], from: position1, to: position2})
					#move = Move.create!({text: gam.moves[i-1], from: position1, to: position2})
					position1 = position2
					#game_moves << {move_id: move.id, game_id: game.id, half_move_number: i}
					#GameMove.create!({move_id: move.id, game_id: game.id, half_move_number: i})
				end
			end
			# File.write("game_positions.txt", positions_to_add.map { |x| "#{x}\r" }.join)
			# ActiveRecord::Base.connection.execute "COPY positions (fen) FROM 'C:\\Project.GameDB\\DBWorker\\game_positions.txt'"
			# File.write("game_moves.txt", game_moves.map { |x| "#{x[:move_id]}\t#{x[:game_id]}\t#{x[:half_move_number]}\r" }.join)
			# ActiveRecord::Base.connection.execute "COPY game_moves (move_id, game_id, half_move_number) FROM 'C:\\Project.GameDB\\DBWorker\\game_moves.txt'"
			#puts "Inserted game with id #{game.id}\n"
		end
		total_time = Time.now - start_time;
		puts "Parsing finished in #{parse_time}\nLoaded to DB in     #{Time.now-base_time}\nTotal time          #{total_time}"
		puts "Time per game: #{total_time/games.length}"
		return total_time
	end
=end

#DBWorker.parse('../testing/lol.pgn')

end