#!/usr/bin/ruby
#encoding utf-8
$LOAD_PATH << '../../pgn/lib'

require 'rubygems'
require 'active_record'
require 'yaml'
require 'pgn'

load 'binary_search_tree.rb'
load 'binary_search_tree_hash.rb'

class DBWorker



	#Инициализация соединения с базой данных
	def initialize()
		Dir["model/*.rb"].each { |file| load(file) }
		Dir["db/migrate/*.rb"].each { |file| load(file) }
		config = File.open('config/database.yml')
		@@dbconfig = YAML::load(config)
		config.close()
		ActiveRecord::Base.logger = nil
		ActiveRecord::Base.establish_connection(@@dbconfig)
		ActiveRecord::Migrator.migrate 'db/migrate', nil
	end


	#Очистка базы данных
	def remakeDB()
		puts "Dropping DB #{@@dbconfig['database']}\n"
		#Обрыв всех настоящих подключений к базе
		terminate_sql = <<-eos
			SELECT pg_terminate_backend(pg_stat_activity.pid)
			FROM   pg_stat_activity
			WHERE  pg_stat_activity.datname = \'#{@@dbconfig['database']}\'
	  			   AND pid <> pg_backend_pid();
	  	eos
		begin
		  	ActiveRecord::Base.connection.execute terminate_sql
		  	puts "Disconnected others"
		rescue Exception => e
			puts "Failed to disconnect due to: #{e}"
		end
		ActiveRecord::Base.establish_connection(@@dbconfig.merge('database' => 'postgres'))
		ActiveRecord::Base.connection.drop_database @@dbconfig['database']
		puts "Creating DB #{@@dbconfig['database']}\n"
		ActiveRecord::Base.connection.create_database(@@dbconfig['database'])
		ActiveRecord::Base.establish_connection(@@dbconfig)
		ActiveRecord::Migrator.migrate 'db/migrate', nil
	end


	def truncateDB()
		puts "\nTruncating table game_files ..."
		ActiveRecord::Base.connection.execute "truncate table game_files"
		puts "\nTruncating table game_moves ..."
		ActiveRecord::Base.connection.execute "truncate table game_moves"
		puts "\nTruncating table game_tags ..."
		ActiveRecord::Base.connection.execute "truncate table game_tags"
		puts "\nTruncating table games ..."
		ActiveRecord::Base.connection.execute "truncate table games"
		puts "\nTruncating table moves ..."
		ActiveRecord::Base.connection.execute "truncate table moves"
		puts "\nTruncating table positions ..."
		ActiveRecord::Base.connection.execute "truncate table positions"
		puts "\nTruncating table tags ..."
		ActiveRecord::Base.connection.execute "truncate table tags"
		puts "\nTruncation complete!"
	end

	def truncateDB_big()
		puts "\nTruncating table game_files ..."
		ActiveRecord::Base.connection.execute "truncate table game_files"
		puts "\nTruncating table game_moves ..."
		ActiveRecord::Base.connection.execute "truncate table game_moves"
		puts "\nTruncating table game_tags ..."
		ActiveRecord::Base.connection.execute "truncate table game_tags"
		puts "\nTruncating table games ..."
		ActiveRecord::Base.connection.execute "truncate table games"
		(2..32).each do |n|
			['w','b'].each do |color|
				puts "\nTruncating table moves_#{n}_#{color} ..."
				ActiveRecord::Base.connection.execute "truncate table moves_#{n}_#{color}"
				puts "\nTruncating table positions_#{n}_#{color} ..."
				ActiveRecord::Base.connection.execute "truncate table positions_#{n}_#{color}"
			end
		end
		puts "\nTruncating table tags ..."
		ActiveRecord::Base.connection.execute "truncate table tags"
		puts "\nTruncation complete!"
	end


	#Метод, принимающий путь к файлу *.pgn для парсинга и последующей обработки и заливки в базу данных
	def processFile(filename, step = 2000000)
		#Отключаем сборщик мусора
		GC.disable

		@@game_next_id = Game.last.try(:id) || 1
		@@tag_next_id  = Tag.last.try(:id)  || 1	
		@@move_next_id = Move.last.try(:id) || 1
		#@@pos_next_id =  KnownPosition.last.try(:id) || 1
		@@shitty_games_count = 0
		@@games_count = 0

		#experimental
		@@known_positions = BinarySearchTreeHash.new()
		KnownPosition.all.select('id,fen').each do |x|
			@@known_positions[x.fen] = x
		end
		@@pos_next_id = @@known_positions.map { |n| n[1].id }.max + 1

		puts "\nNext position identifier is set to #{@@pos_next_id}"

		@@games_temp_filename = 'games.new'
		@@game_files_temp_filename = 'game_files.new'
		@@game_moves_temp_filename = 'game_moves.new'
		@@game_tags_temp_filename = 'game_tags.new'
		@@moves_temp_filename = 'moves.new'
		@@positions_temp_filename = 'positions.new'
		@@tags_temp_filename = 'tags.new'

		position = 0

		start_time = Time.now

		while(file = File.read(filename, step, position))
			if(file.length<step)
				file += "\n\n\["
			end
			start = nil
			start_next = nil
			matchdata = /\r?\n\r?\n\[/.match(file)
			while !matchdata.nil?
				start = matchdata.begin(0)
				start_next = start + matchdata[0].length - 1
				matchdata = /\r?\n\r?\n\[/.match(file, start_next)
			end

			@@games = []
			@@lengths = [position]
			@@fen_lists = []

			GC.disable

			parse_time = parse(file[0..start.to_i])
			print "\nParsing of (#{@@games_count};#{@@games_count+@@games.count}] games complete in #{parse_time}"
			print "\nTime per game #{parse_time/@@games.count}"

			GC.enable
			GC.start
			GC.disable


			process_time = process_experimental(filename)
			print "\nProcessing of (#{@@games_count};#{@@games_count+@@games.count}] games complete in #{process_time}"
			print "\nTime per game #{process_time/@@games.count}"

			GC.enable
			GC.start
			GC.disable

			upload_time = upload()
			print "\nUploading of (#{@@games_count};#{@@games_count+@@games.count}] games complete in #{upload_time}"
			print "\nTime per game #{upload_time/@@games.count}"

			@@games_count += @@games.count
			position = position + start_next

			GC.enable
			GC.start
		end

		end_time = Time.now
		duration = end_time - start_time
		puts "\nTotal time: #{duration}"
		puts "\nTime per game: #{duration/(@@games_count+@@shitty_games_count)}"
		puts "\nNumber of shitty games: #{@@shitty_games_count}"
		GC.enable
		GC.start
		return duration
	end



	def processFile_big(filename, step = 2000000)
		#Отключаем сборщик мусора
		GC.disable

		@@game_next_id = Game.last.try(:id) || 1
		@@tag_next_id  = Tag.last.try(:id)  || 1	
		#@@move_next_id = Move.last.try(:id) || 1
		@@move_next_id = 1
		#@@pos_next_id =  KnownPosition.last.try(:id) || 1
		@@shitty_games_count = 0
		@@games_count = 0

		#experimental
		@@known_positions = BinarySearchTreeHash.new()
		KnownPosition.all.select('id,fen').each do |x|
			@@known_positions[x.fen] = x
		end
		@@pos_next_id = @@known_positions.map { |n| n[1].id }.max.to_i + 1

		puts "\nNext position identifier is set to #{@@pos_next_id}"

		@@games_temp_filename = 'games.new'
		@@game_files_temp_filename = 'game_files.new'
		@@game_moves_temp_filename = 'game_moves.new'
		@@game_tags_temp_filename = 'game_tags.new'
		@@moves_temp_filename = 'moves'
		@@positions_temp_filename = 'positions'
		@@tags_temp_filename = 'tags.new'

		position = 0

		start_time = Time.now

		while(file = File.read(filename, step, position))
			if(file.length<step)
				file += "\n\n\["
			end
			start = nil
			start_next = nil
			matchdata = /\r?\n\r?\n\[/.match(file)
			while !matchdata.nil?
				start = matchdata.begin(0)
				start_next = start + matchdata[0].length - 1
				matchdata = /\r?\n\r?\n\[/.match(file, start_next)
			end

			@@games = []
			@@lengths = [position]
			@@fen_lists = []
			@@pos_lists = [[]]

			GC.disable

			parse_time = parse(file[0..start.to_i])
			print "\nParsing of (#{@@games_count};#{@@games_count+@@games.count}] games complete in #{parse_time}"
			print "\nTime per game #{parse_time/@@games.count}"

			GC.enable
			GC.start
			GC.disable


			process_time = process_big(filename)
			print "\nProcessing of (#{@@games_count};#{@@games_count+@@games.count}] games complete in #{process_time}"
			print "\nTime per game #{process_time/@@games.count}"

			GC.enable
			GC.start
			GC.disable

			upload_time = upload_big()
			print "\nUploading of (#{@@games_count};#{@@games_count+@@games.count}] games complete in #{upload_time}"
			print "\nTime per game #{upload_time/@@games.count}"

			@@games_count += @@games.count
			position = position + start_next

			GC.enable
			GC.start
		end

		end_time = Time.now
		duration = end_time - start_time
		puts "\nTotal time: #{duration}"
		puts "\nTime per game: #{duration/(@@games_count+@@shitty_games_count)}"
		puts "\nNumber of shitty games: #{@@shitty_games_count}"
		GC.enable
		GC.start
		return duration
	end



	#Метод, парсящий игры
	def parse(games_text, step = 80000)
		GC.enable
		GC.start
		GC.disable

		#Открываем файл для вывода непарсящихся по каким-либо причинам игр
		shitty_games_file = File.open("shitty_games.shit","a")

		offset = 0

		start_time = Time.now
		print "\n"
		while((file = games_text[offset..offset+step]) != nil)

			if file.length<step
				file += "\n\n\["
			end

			start = nil
			start_next = 0
			matchdata = /\r?\n\r?\n\[/.match(file,0)
			start = matchdata[0].length - 1
			matchdata = /\r?\n\r?\n\[/.match(file,start)
			new_game = nil


			while !matchdata.nil?

				begin
					new_game = PGN.parse(file[start_next..matchdata.begin(0)])[0]
					@@fen_lists << new_game.fen_list
				rescue Exception => e
					puts e.inspect
					new_game = nil
					@@fen_lists << nil
					shitty_games_file.write("#{file[start_next..matchdata.begin(0)]}\r\r")
					@@shitty_games_count = @@shitty_games_count + 1
					print "\n.................SHIT MAFU..............\n#{file[start_next..matchdata.begin(0)]}........................................\n"
					print "\r!!!!Shitty game detected on [#{start_next};#{matchdata.begin(0)}] position!!!!\n"
				end


				start = matchdata.begin(0)
				start_next = start + matchdata[0].length - 1
				@@lengths << @@lengths[0]+start_next + offset - 1
				matchdata = /\r?\n\r?\n\[/.match(file, start_next)
				@@games << new_game
			end

			if start.nil?
				puts "End of file =("
				break
			end

			GC.enable
			GC.start
			GC.disable
			offset = offset + start_next
			print "\rParsed up to #{@@lengths.last} position"
		end
		@@fen_lists.each_with_index do |fen_list,index|
			if fen_list
				@@pos_lists[index] = Array.new
				fen_list.each_with_index do |fen, ind|
					@@pos_lists[index][ind] = fen.to_position
				end
			end
		end
		shitty_games_file.close
		file = nil
		GC.enable
		GC.start
		return (Time.now - start_time)

	end




	#Метод, обрабатывающий распарсенные игры под модели и пишущий их в файлы
	def process(filename)
		GC.enable
		GC.start
		GC.disable

		print "\n"

		gamefile = GameFile.create!({file_path: filename})
		
		start_time = Time.now

		all_tags = BinarySearchTreeHash.new()

		Tag.all.select('id,name').each do |x|
			all_tags[x.name] = x
		end

		new_tags = BinarySearchTreeHash.new()

		new_games = []
		new_game_tags = []
		new_game_moves = []

		positions_file = File.open(@@positions_temp_filename, "w")
		moves_file = File.open(@@moves_temp_filename, "w")
		game_moves_file = File.open(@@game_moves_temp_filename, "w")

		@@games.each_with_index do |gam,ind|
			if gam

				game = Game.new({id:@@game_next_id, game_file_id: gamefile.id, offset: @@lengths[ind],length: @@lengths[ind+1]-@@lengths[ind]})
				if gam.result == '0-1'
					game.result = -1
				elsif gam.result == '1-0'
					game.result = 1
				else game.result = 0
				end
				new_games << game

				gam.tags.each do |tg|

					if !(tag = all_tags[tg[0]] || new_tags[tg[0]])
						tag = Tag.new({id:@@tag_next_id,name:tg[0]})
						@@tag_next_id = @@tag_next_id + 1
						new_tags[tag.name] = tag
					end

					new_game_tags << {tag_id: tag.id, game_id: game.id, tag_value: tg[1]}

				end

				@@game_next_id = @@game_next_id + 1

				fenhash = 0
				positions_file.write("#{@@pos_next_id}\t#{@@fen_lists[ind][0]}\t0\r") 
				position_1_id = @@pos_next_id
				@@pos_next_id = @@pos_next_id + 1
				
				@@fen_lists[ind].each_with_index do |fn,i|

					if i>0

						position2 = nil
						positions_file.write("#{@@pos_next_id}\t#{fn}\t0\r")
						@@move_next_id = @@move_next_id + 1
						moves_file.write("#{@@move_next_id}\t#{position_1_id}\t#{@@pos_next_id}\t#{gam.moves[i-1]}\r")
						position_1_id = @@pos_next_id
						game_moves_file.write("#{@@move_next_id}\t#{game.id}\t#{i}\r")
						@@pos_next_id = @@pos_next_id + 1


					end

				end

			end

			print "\rProcessing Game #{@@games_count+ind}"\

		end

		print "\nWriting games to file"
		File.write(@@games_temp_filename, new_games.map { |x| "#{x[:id]}\t#{x[:game_file_id]}\t#{x[:offset]}\t#{x[:length]}\t#{x[:result]}\r" }.join)
		print "\nWriting tags to file"
		File.write(@@tags_temp_filename, new_tags.values.map { |x| "#{x[:id]}\t#{x[:name]}\r" }.join)
		print "\nWriting game tags to file"
		File.write(@@game_tags_temp_filename, new_game_tags.map { |x| "#{x[:game_id]}\t#{x[:tag_id]}\t#{x[:tag_value]}\r" }.join)
		print "\nWriting positions to file"
		positions_file.close
		print "\nWriting moves to file"
		moves_file.close
		print "\nWriting game moves to file"
		game_moves_file.close

		GC.enable
		GC.start
		return (Time.now - start_time)
	end


	
		def process_big(filename)
		GC.enable
		GC.start
		GC.disable

		print "\n"

		gamefile = GameFile.create!({file_path: filename})
		
		start_time = Time.now

		all_tags = BinarySearchTreeHash.new()

		Tag.all.select('id,name').each do |x|
			all_tags[x.name] = x
		end

		new_tags = BinarySearchTreeHash.new()

		new_games = []
		new_game_tags = []
		new_game_moves = []

		game_moves_file = File.open(@@game_moves_temp_filename, "w")
		positions_file = {'w'=>[],'b'=>[]}
		moves_file = {'w'=>[],'b'=>[]}

		['w','b'].each do |color|
			(2..32).each do |n|	
				positions_file[color][n] = File.open(@@positions_temp_filename+"_#{n}_#{color}.new", "w")
				moves_file[color][n] = File.open(@@moves_temp_filename+"_#{n}_#{color}.new", "w")
			end
		end

		@@games.each_with_index do |gam,ind|
			if gam

				game = Game.new({id:@@game_next_id, game_file_id: gamefile.id, offset: @@lengths[ind],length: @@lengths[ind+1]-@@lengths[ind]})
				if gam.result == '0-1'
					game.result = -1
				elsif gam.result == '1-0'
					game.result = 1
				else game.result = 0
				end
				new_games << game

				gam.tags.each do |tg|

					if !(tag = all_tags[tg[0]] || new_tags[tg[0]])
						tag = Tag.new({id:@@tag_next_id,name:tg[0]})
						@@tag_next_id = @@tag_next_id + 1
						new_tags[tag.name] = tag
					end

					new_game_tags << {tag_id: tag.id, game_id: game.id, tag_value: tg[1]}

				end

				@@game_next_id = @@game_next_id + 1

				first_fen = @@fen_lists[ind][0]
				first_pos = @@pos_lists[ind][0]

				if @@known_positions[first_fen.to_short_s]
					position_1_fen = first_fen
					position_1_id = @@known_positions[first_fen.to_short_s].id
					position_1_pos = first_fen.to_position
				else
					positions_file[first_fen.active][first_pos.figures_count].write("#{@@pos_next_id}\t#{first_fen.to_short_s}\r") 
					position_1_fen = first_fen
					position_1_pos = first_pos
					position_1_id = @@pos_next_id
					@@pos_next_id = @@pos_next_id + 1
				end
				@@fen_lists[ind].each_with_index do |fn,i|

					if i>0

						if @@known_positions[fn.to_short_s]
							position_2_fen = fn
							position_2_pos = fn.to_position
							position_2_id = @@known_positions[fn.to_short_s].id
						else
							positions_file[fn.active][@@pos_lists[ind][i].figures_count].write("#{@@pos_next_id}\t#{fn.to_short_s}\r")
							position_2_fen = fn
							position_2_pos = @@pos_lists[ind][i]
							position_2_id = @@pos_next_id
							@@pos_next_id = @@pos_next_id + 1
						end
=begin
						if (position_1_id<@@pos_next_id && position_2_id<@@pos_next_id)
							game_moves_file.write("#{@@move_next_id}\t#{game.id}\t#{i}\r")
						else
							@@pos_next_id = @@pos_next_id + 1
							positions_file.write("#{@@pos_next_id}\t#{@@fen_lists[ind][0]}\t0\r") 
							position_1_id = @@pos_next_id
						end
=end
						moves_file[position_1_fen.active][position_1_pos.figures_count].write("#{@@move_next_id}\t#{position_1_id}\t#{position_2_id}\t#{position_2_pos.figures_count}_#{position_2_fen.active}\t#{gam.moves[i-1]}\r")
						game_moves_file.write("#{@@move_next_id}\t#{position_2_pos.figures_count}_#{position_2_fen.active}\t#{game.id}\t#{i}\r")

						@@move_next_id = @@move_next_id + 1

						position_1_id = position_2_id
						position_1_fen = position_2_fen
					end

				end

			end

			print "\rProcessing Game #{@@games_count+ind}"

		end

		print "\nWriting games to file"
		File.write(@@games_temp_filename, new_games.map { |x| "#{x[:id]}\t#{x[:game_file_id]}\t#{x[:offset]}\t#{x[:length]}\t#{x[:result]}\r" }.join)
		print "\nWriting tags to file"
		File.write(@@tags_temp_filename, new_tags.values.map { |x| "#{x[:id]}\t#{x[:name]}\r" }.join)
		print "\nWriting game tags to file"
		File.write(@@game_tags_temp_filename, new_game_tags.map { |x| "#{x[:game_id]}\t#{x[:tag_id]}\t#{x[:tag_value]}\r" }.join)
		print "\nWriting positions to file"
		positions_file.each_value { |arr| arr.each {|f| f.close if f } }
		print "\nWriting moves to file"
		moves_file.each_value { |arr| arr.each { |f| f.close if f } }
		print "\nWriting game moves to file"
		game_moves_file.close

		GC.enable
		GC.start
		return (Time.now - start_time)
	end




	def self.process_experimental(filename)
		GC.enable
		GC.start
		GC.disable

		print "\n"

		gamefile = GameFile.create!({file_path: filename})
		
		start_time = Time.now

		all_tags = BinarySearchTreeHash.new()

		Tag.all.select('id,name').each do |x|
			all_tags[x.name] = x
		end

		new_tags = BinarySearchTreeHash.new()

		new_games = []
		new_game_tags = []
		new_game_moves = []

		positions_file = File.open(@@positions_temp_filename, "w")
		moves_file = File.open(@@moves_temp_filename, "w")
		game_moves_file = File.open(@@game_moves_temp_filename, "w")

		@@games.each_with_index do |gam,ind|
			if gam

				game = Game.new({id:@@game_next_id, game_file_id: gamefile.id, offset: @@lengths[ind],length: @@lengths[ind+1]-@@lengths[ind]})
				if gam.result == '0-1'
					game.result = -1
				elsif gam.result == '1-0'
					game.result = 1
				else game.result = 0
				end
				new_games << game

				gam.tags.each do |tg|

					if !(tag = all_tags[tg[0]] || new_tags[tg[0]])
						tag = Tag.new({id:@@tag_next_id,name:tg[0]})
						@@tag_next_id = @@tag_next_id + 1
						new_tags[tag.name] = tag
					end

					new_game_tags << {tag_id: tag.id, game_id: game.id, tag_value: tg[1]}

				end

				@@game_next_id = @@game_next_id + 1

				fenhash = 0
				if @@known_positions[@@fen_lists[ind][0]]
					position_1_id = @@known_positions[@@fen_lists[ind][0]].id
				else
					positions_file.write("#{@@pos_next_id}\t#{@@fen_lists[ind][0]}\t0\r") 
					position_1_id = @@pos_next_id
					@@pos_next_id = @@pos_next_id + 1
				end
				@@fen_lists[ind].each_with_index do |fn,i|

					if i>0

						position2 = nil
						if @@known_positions[fn]
							position_2_id = @@known_positions[fn].id
						else
							positions_file.write("#{@@pos_next_id}\t#{fn}\t0\r") 
							position_2_id = @@pos_next_id
							@@pos_next_id = @@pos_next_id + 1
						end
=begin
						if (position_1_id<@@pos_next_id && position_2_id<@@pos_next_id)
							game_moves_file.write("#{@@move_next_id}\t#{game.id}\t#{i}\r")
						else
							@@pos_next_id = @@pos_next_id + 1
							positions_file.write("#{@@pos_next_id}\t#{@@fen_lists[ind][0]}\t0\r") 
							position_1_id = @@pos_next_id
						end
=end
						moves_file.write("#{@@move_next_id}\t#{position_1_id}\t#{position_2_id}\t#{gam.moves[i-1]}\r")
						game_moves_file.write("#{@@move_next_id}\t#{game.id}\t#{i}\r")

						@@move_next_id = @@move_next_id + 1
						position_1_id = position_2_id

					end

				end

			end

			print "\rProcessing Game #{@@games_count+ind}"\

		end

		print "\nWriting games to file"
		File.write(@@games_temp_filename, new_games.map { |x| "#{x[:id]}\t#{x[:game_file_id]}\t#{x[:offset]}\t#{x[:length]}\t#{x[:result]}\r" }.join)
		print "\nWriting tags to file"
		File.write(@@tags_temp_filename, new_tags.values.map { |x| "#{x[:id]}\t#{x[:name]}\r" }.join)
		print "\nWriting game tags to file"
		File.write(@@game_tags_temp_filename, new_game_tags.map { |x| "#{x[:game_id]}\t#{x[:tag_id]}\t#{x[:tag_value]}\r" }.join)
		print "\nWriting positions to file"
		positions_file.close
		print "\nWriting moves to file"
		moves_file.close
		print "\nWriting game moves to file"
		game_moves_file.close

		GC.enable
		GC.start
		return (Time.now - start_time)
	end




	#Метод, обрабатывающий распарсенные игры под модели и пишущий их в файлы, проверяя уникальность в базе данных
	def process_unique(filename)
		GC.enable
		GC.start
		GC.disable

		print "\n"

		gamefile = GameFile.create!({file_path: filename})
		
		start_time = Time.now

		all_tags = BinarySearchTreeHash.new()

		Tag.all.select('id,name').each do |x|
			all_tags[x.name] = x
		end

		new_tags = BinarySearchTreeHash.new()

		all_pos = BinarySearchTreeHash.new()
		Position.all.select('id,fen_hash').each do |x|
			all_pos[x.fen_hash] = x
		end
		new_pos = BinarySearchTreeHash.new()
		all_moves = BinarySearchTreeHash.new()
		Move.all.select('id,from_id,to_id').each do |x|
			all_moves["#{x.from_id};#{x.to_id}"] = x
		end
		new_moves = BinarySearchTreeHash.new()

		new_games = []
		new_game_tags = []
		new_game_moves = []

		game_moves_file = File.open(@@game_moves_temp_filename, "w")
		positions_file = File.open(@@positions_temp_filename, "w")
		moves_file = File.open(@@moves_temp_filename, "w")


		@@games.each_with_index do |gam,ind|
			if gam

				game = Game.new({id:@@game_next_id, game_file_id: gamefile.id, offset: @@lengths[ind],length: @@lengths[ind+1]-@@lengths[ind]})
				if gam.result == '0-1'
					game.result = -1
				elsif gam.result == '1-0'
					game.result = 1
				else game.result = 0
				end
				new_games << game

				gam.tags.each do |tg|

					if !(tag = all_tags[tg[0]] || new_tags[tg[0]])
						tag = Tag.new({id:@@tag_next_id,name:tg[0]})
						@@tag_next_id = @@tag_next_id + 1
						new_tags[tag.name] = tag
					end

					new_game_tags << {tag_id: tag.id, game_id: game.id, tag_value: tg[1]}

				end

				@@game_next_id = @@game_next_id + 1

				fenhash = @@fen_lists[ind][0].hash
				positions_to_add = []
				position1 = Position.where({fen_hash: fenhash}).first || Position.create!({fen: @@fen_lists[ind][0], fen_hash: fenhash})
				if !(position1 = all_pos[fenhash] || new_pos[fenhash])
					position1 = Position.new({id: @@pos_next_id, fen: @@fen_lists[ind][0], fen_hash: fenhash})
					new_pos[fenhash] = position1
					positions_file.write("#{@@pos_next_id}\t#{@@fen_lists[ind][0]}\t0\r")
					@@pos_next_id = @@pos_next_id + 1
				end
				if position1.nil?
					positions_to_add << @@fen_lists[ind][0]
				end 
				position_1_id = @@pos_next_id
				
				@@fen_lists[ind].each_with_index do |fn,i|

					if i>0

						position2 = nil
						fenhash = fn.hash
						if !(position2 = all_pos[fenhash] || new_pos[fenhash])
							position2 = Position.new({id: @@pos_next_id, fen: fn, fen_hash: fenhash})
							new_pos[fenhash] = position2
							positions_file.write("#{@@pos_next_id}\t#{fn}\t0\r")
							@@pos_next_id = @@pos_next_id + 1
						end
						if !(move = all_moves["#{position1.id};#{position2.id}"] || new_moves["#{position1.id};#{position2.id}"])
							move = Move.new({id: @@move_next_id, from_id: position1.id, to_id: position2.id, text: gam.moves[i-1]})
							@@move_next_id = @@move_next_id + 1
							new_moves["#{position1.id};#{position2.id}"] = move
							moves_file.write("#{@@move_next_id}\t#{position_1_id}\t#{@@pos_next_id}\t#{gam.moves[i-1]}\r")
						end
						position_1_id = @@pos_next_id
						game_moves_file.write("#{@@move_next_id}\t#{game.id}\t#{i}\r")
						new_game_moves << {move_id: @@move_next_id, game_id: game.id, half_move_number: i}
						GameMove.create!({move_id: move.id, game_id: game.id, half_move_number: i})
					
					end

				end

			end

			print "\rProcessing Game #{@@games_count+ind}"\

		end

		print "\nWriting games to file"
		File.write(@@games_temp_filename, new_games.map { |x| "#{x[:id]}\t#{x[:game_file_id]}\t#{x[:offset]}\t#{x[:length]}\t#{x[:result]}\r" }.join)
		print "\nWriting tags to file"
		File.write(@@tags_temp_filename, new_tags.values.map { |x| "#{x[:id]}\t#{x[:name]}\r" }.join)
		print "\nWriting game tags to file"
		File.write(@@game_tags_temp_filename, new_game_tags.map { |x| "#{x[:game_id]}\t#{x[:tag_id]}\t#{x[:tag_value]}\r" }.join)
		print "\nWriting positions to file"
		positions_file.close
		print "\nWriting moves to file"
		moves_file.close
		print "\nWriting game moves to file"
		game_moves_file.close

		GC.enable
		GC.start
		return (Time.now - start_time)
	end



	#Метод, заливающий обработанные игры в базу данных пачками
	def upload()
		GC.enable
		GC.start
		GC.disable

		start_time = Time.now

		print "\nMass insert of games to DB"
		ActiveRecord::Base.connection.execute "COPY games (id, game_file_id, \"offset\", length, result) FROM 'C:\\Project.GameDB\\DBWorker\\games.new'"
		print "\nMass insert of tags to DB"
		ActiveRecord::Base.connection.execute "COPY tags (id, name) FROM 'C:\\Project.GameDB\\DBWorker\\tags.new'"
		print "\nMass insert of game tags to DB"
		ActiveRecord::Base.connection.execute "COPY game_tags (game_id, tag_id, tag_value) FROM 'C:\\Project.GameDB\\DBWorker\\game_tags.new'"
		print "\nMass insert of positions to DB"
		ActiveRecord::Base.connection.execute "COPY positions (id, fen, fen_hash) FROM 'C:\\Project.GameDB\\DBWorker\\positions.new'"
		print "\nMass insert of moves to DB"
		ActiveRecord::Base.connection.execute "COPY moves (id, from_id, to_id,text) FROM 'C:\\Project.GameDB\\DBWorker\\moves.new'"
		print "\nMass insert of game moves to DB"
		ActiveRecord::Base.connection.execute "COPY game_moves (move_id, game_id, half_move_number) FROM 'C:\\Project.GameDB\\DBWorker\\game_moves.new'"

		GC.enable
		GC.start

		return (Time.now - start_time)
	end



	def upload_big()
		GC.enable
		GC.start
		GC.disable

		start_time = Time.now

		print "\nMass insert of games to DB"
		ActiveRecord::Base.connection.execute "COPY games (id, game_file_id, \"offset\", length, result) FROM 'C:\\Project.GameDB\\DBWorker\\games.new'"
		print "\nMass insert of tags to DB"
		ActiveRecord::Base.connection.execute "COPY tags (id, name) FROM 'C:\\Project.GameDB\\DBWorker\\tags.new'"
		print "\nMass insert of game tags to DB"
		ActiveRecord::Base.connection.execute "COPY game_tags (game_id, tag_id, tag_value) FROM 'C:\\Project.GameDB\\DBWorker\\game_tags.new'"
		print "\nMass insert of game moves to DB"
		ActiveRecord::Base.connection.execute "COPY game_moves (move_id, move_table_id, game_id, half_move_number) FROM 'C:\\Project.GameDB\\DBWorker\\game_moves.new'"
		(2..32).each do |n|
			['w','b'].each do |color|
				print "\nMass insert of positions_#{n}_#{color} to DB"
				ActiveRecord::Base.connection.execute "COPY positions_#{n}_#{color} (id, fen) FROM 'C:\\Project.GameDB\\DBWorker\\positions_#{n}_#{color}.new'"
				print "\nMass insert of moves_#{n}_#{color} to DB"
				ActiveRecord::Base.connection.execute "COPY moves_#{n}_#{color} (id, from_id, to_id, to_table_id, text) FROM 'C:\\Project.GameDB\\DBWorker\\moves_#{n}_#{color}.new'"
			end
		end

		GC.enable
		GC.start

		return (Time.now - start_time)
	end



	def same_figures_positions(filename, step = 2000000)
		#Отключаем сборщик мусора
		GC.disable
		@@shitty_games_count = 0
		@@games_count = 0
		@@same_figures_positions = {':white'=>[],':black'=>[]}
		@@fen_listis = []

		#experimental
		@@known_positions = BinarySearchTreeHash.new()
		KnownPosition.all.select('id,fen').each do |x|
			@@known_positions[x.fen] = x
		end

		position = 0

		start_time = Time.now

		while(file = File.read(filename, step, position))
			if(file.length<step)
				file += "\n\n\["
			end
			start = nil
			start_next = nil
			matchdata = /\r?\n\r?\n\[/.match(file)
			while !matchdata.nil?
				start = matchdata.begin(0)
				start_next = start + matchdata[0].length - 1
				matchdata = /\r?\n\r?\n\[/.match(file, start_next)
			end

			@@games = []
			@@lengths = [position]
			@@fen_lists = []

			GC.disable

			parse_time = DBWorker::parse(file[0..start.to_i])
			print "\nParsing of (#{@@games_count};#{@@games_count+@@games.count}] games complete in #{parse_time}"
			print "\nTime per game #{parse_time/@@games.count}"

			@@games.each_with_index do |g,i|
				if g
					g.positions.each_with_index do |p,ind|

						if !@@known_positions[@@fen_lists[i][ind]]
							fig_count = p.figures_count
							@@same_figures_positions[p.player.inspect][fig_count.to_i] = @@same_figures_positions[p.player.inspect][fig_count.to_i].to_i + 1
						end

					end
				end

			end

			GC.enable
			GC.start
			GC.disable

			@@games_count += @@games.count
			position = position + start_next

			GC.enable
			GC.start
		end

		end_time = Time.now
		duration = end_time - start_time
		puts "\nTotal time: #{duration}"
		puts "\nTime per game: #{duration/(@@games_count+@@shitty_games_count)}"
		puts "\nNumber of shitty games: #{@@shitty_games_count}"

		f = File.open("#{filename.gsub(/\.[^\.]*$/,'')+'_stat.txt'}",'w')

		f.write("N\t|\tWHITE\t|\tBLACK\r")
		f.write("-------------------------------------\r")
		for i in 2..32 do
			f.write("#{i}\t|\t#{@@same_figures_positions.percentage[':white'][i]}\t|\t#{@@same_figures_positions.percentage[':black'][i]}\r")
		end
=begin
		@@same_figures_positions[":white"].percentage.each_with_index do |p,i|
			f.write("#{i}\t#{p.to_i}\r") if i>1
		end
		f.write("BLACK's moves:\r\n")
		@@same_figures_positions[":black"].percentage.each_with_index do |p,i|
			f.write("#{i}\t#{p.to_i}\r") if i>1
		end
=end
		f.close

		GC.enable
		GC.start
		return @@same_figures_positions
	end


end

class Hash
	def percentage()
		@result = {':white'=>[],':black'=>[]}
		@total = 0

		self.each_value do |val|
			val.each do |v|
				@total = @total.to_i + v.to_i
			end
		end
		self[':white'].each_with_index do |val, ind|
			@result[':white'][ind] = (val.to_i*100.0/@total.to_i).round(2)
		end
		self[':black'].each_with_index do |val, ind|
			@result[':black'][ind] = (val.to_i*100.0/@total.to_i).round(2)
		end
		return @result
	end
end
