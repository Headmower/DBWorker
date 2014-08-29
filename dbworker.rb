#!/usr/bin/ruby
#encoding utf-8
$LOAD_PATH << '../../pgn/lib'

require 'rubygems'
require 'active_record'
require 'yaml'
require 'pgn'

load 'binary_search_tree.rb'
load 'binary_search_tree_hash.rb'

load 'dbworker/parser.rb'
load 'dbworker/extra.rb'
load 'dbworker/processor.rb'
load 'dbworker/uploader.rb'
module DBWorker

	#Инициализация соединения с базой данных
	def self.extended(obj)
		Dir["model/*.rb"].each { |file| load(file) }
		Dir["db/migrate/*.rb"].each { |file| load(file) }
		config = File.open('config/database.yml')
		@@dbconfig = YAML::load(config)
		config.close()
		ActiveRecord::Base.logger = nil
		ActiveRecord::Base.establish_connection(@@dbconfig)
		
		migrate

	end

	def self.migrate()
		ActiveRecord::Migrator.migrate 'db/migrate', nil
	end


	#Очистка базы данных
	def self.remakeDB()
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


	def self.truncateDB()
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

	def self.truncateDB_big()
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
	def self.processFile(filename, step = 2000000)
		#Отключаем сборщик мусора
		GC.disable

		@@game_next_id = Game.last.try(:id) || 1
		@@tag_next_id  = Tag.last.try(:id)  || 1
		@@move_next_id = Move.last.try(:id) || 1
		#@@pos_next_id =  Position.last.try(:id) || 1
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

			parse_time = self.parse(file[0..start.to_i])
			print "\nParsing of (#{@@games_count};#{@@games_count+@@games.count}] games complete in #{parse_time}"
			print "\nTime per game #{parse_time/@@games.count}"

			GC.enable
			GC.start
			GC.disable


			process_time = self.process(filename)
			print "\nProcessing of (#{@@games_count};#{@@games_count+@@games.count}] games complete in #{process_time}"
			print "\nTime per game #{process_time/@@games.count}"

			GC.enable
			GC.start
			GC.disable

			upload_time = self.upload()
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



	def self.processFile_big(filename, step = 2000000)
		#Отключаем сборщик мусора
		GC.disable

		@@game_next_id = Game.last.try(:id).to_i + 1
		@@tag_next_id  = Tag.last.try(:id).to_i + 1 

		@@move_next_id = Move.max_id.to_i + 1
		@@pos_next_id =  Position.max_id.to_i + 1

		@@shitty_games_count = 0
		@@games_count = 0

		#experimental
		@@known_positions = BinarySearchTreeHash.new()
		KnownPosition.all.select('id,fen').each do |x|
			@@known_positions[x.fen] = x
		end
		#@@pos_next_id = @@known_positions.map { |n| n[1].id }.max.to_i + 1

		puts "\nNext position identifier is set to #{@@pos_next_id}"
		puts "\nNext move identifier is set to #{@@move_next_id}"

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

			GC.disable

			parse_time = self.parse(file[0..start.to_i])
			print "\nParsing of (#{@@games_count};#{@@games_count+@@games.count}] games complete in #{parse_time}"
			print "\nTime per game #{parse_time/@@games.count}"

			GC.enable
			GC.start
			GC.disable


			process_time = self.process_big(filename)
			print "\nProcessing of (#{@@games_count};#{@@games_count+@@games.count}] games complete in #{process_time}"
			print "\nTime per game #{process_time/@@games.count}"

			GC.enable
			GC.start
			GC.disable

			upload_time = self.upload_big()
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

end

extend DBWorker