module DBWorker


		#Метод, заливающий обработанные игры в базу данных пачками

		def self.upload()
			GC.enable
			GC.start
			GC.disable

			start_time = Time.now

			print "\nMass insert of games to DB"
			ActiveRecord::Base.connection.execute "COPY tagged_games(id, game_file_id, \"offset\", \"length\", result, eco, str_result, black, white, round, \"date\", site, event, blackelo, whiteelo, opening, fen, setup, variation, \"year\", \"month\", \"day\", eco_code, eco_num) FROM 'C:\\DBWorker\\tmp\\games.new'"
			print "\nMass insert of tags to DB"
			ActiveRecord::Base.connection.execute "COPY tags (id, name) FROM 'C:\\DBWorker\\tmp\\tags.new'"
			# print "\nMass insert of game tags to DB"
			# ActiveRecord::Base.connection.execute "COPY game_tags (game_id, tag_id, tag_value) FROM 'C:\\DBWorker\\game_tags.new'"
			print "\nMass insert of positions to DB"
			ActiveRecord::Base.connection.execute "COPY positions (id, table_index, fen) FROM 'C:\\DBWorker\\tmp\\positions.new'"
			print "\nMass insert of moves to DB"
			ActiveRecord::Base.connection.execute "COPY moves (id, table_index, from_id, to_id, text, to_table_index) FROM 'C:\\DBWorker\\tmp\\moves.new'"
			print "\nMass insert of game moves to DB"
			ActiveRecord::Base.connection.execute "COPY game_moves (id, table_index, move_id, game_id, half_move_number) FROM 'C:\\DBWorker\\tmp\\game_moves.new'"

			GC.enable
			GC.start

			return (Time.now - start_time)
		end



		def self.upload_big()
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


end