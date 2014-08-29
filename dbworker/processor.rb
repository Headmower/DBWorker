module DBWorker

		
		#Метод, обрабатывающий распарсенные игры под модели и пишущий их в файлы
		def self.process(filename)
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

			tagged_games_file = File.open(@@tagged_games_temp_filename,"w")
			positions_file = File.open(@@positions_temp_filename, "w")
			moves_file = File.open(@@moves_temp_filename, "w")
			game_moves_file = File.open(@@game_moves_temp_filename, "w")

			@@games.each_with_index do |gam,ind|
				if gam

					tags = Hash.new()
					gam.tags.each_pair do |key,value|
						tags.merge!({key.downcase => value})
					end

					game = TaggedGame.new({
						id:@@game_next_id, game_file_id: gamefile.id, offset: @@lengths[ind],length: @@lengths[ind+1]-@@lengths[ind],
						eco: tags['eco'], str_result: tags['result'], black: tags['black'], white: tags['white'],
						round: tags['round'], date: tags['date'], site: tags['site'], event: tags['event'],
						blackelo: tags['blackelo'].to_i, whiteelo: tags['whiteelo'].to_i, opening: tags['opening'], fen: tags['fen'], setup: tags['setup'],
						variation: tags['variation'], year: tags['year'].to_i, month: tags['month'].to_i, day: tags['day'].to_i,
						eco_code: tags['eco'][0], eco_num: tags['eco'].slice(1..66)
						})
					if gam.result == '0-1'
						game.result = -1
					elsif gam.result == '1-0'
						game.result = 1
					else game.result = 0
					end
					# new_games << game
					tagged_games_file.write("#{game[:id]}\t#{game[:game_file_id]}\t#{game[:offset]}\t#{game[:length]}\t#{game[:result]}\t#{game[:eco]}\t#{game[:str_result]}\t#{game[:black]}\t#{game[:white]}\t#{game[:round]}\t#{game[:date]}\t#{game[:site]}\t#{game[:event]}\t#{game[:blackelo]}\t#{game[:whiteelo]}\t#{game[:opening]}\t#{game[:fen]}\t#{game[:setup]}\t#{game[:variation]}\t#{game[:year]}\t#{game[:month]}\t#{game[:day]}\t#{game[:eco_code]}\t#{game[:eco_num]}\r")

					gam.tags.each do |tg|

						if !(tag = all_tags[tg[0]] || new_tags[tg[0]])
							tag = Tag.new({id:@@tag_next_id,name:tg[0]})
							@@tag_next_id = @@tag_next_id + 1
							new_tags[tag.name] = tag
						end

					# 	new_game_tags << {tag_id: tag.id, game_id: game.id, tag_value: tg[1]}

					end

					@@game_next_id = @@game_next_id + 1

					prev_index = @@fen_lists[ind][0].to_position.figures_count
					positions_file.write("#{@@pos_next_id}\t#{prev_index}\t#{@@fen_lists[ind][0]}\r") 
					position_1_id = @@pos_next_id
					@@pos_next_id = @@pos_next_id + 1
					@@fen_lists[ind].each_with_index do |fn,i|

						if i>0
							cur_index = fn.to_position.figures_count
							position2 = nil
							positions_file.write("#{@@pos_next_id}\t#{cur_index}\t#{fn}\r")
							@@move_next_id = @@move_next_id + 1
							moves_file.write("#{@@move_next_id}\t#{prev_index}\t#{position_1_id}\t#{@@pos_next_id}\t#{gam.moves[i-1]}\t#{cur_index}\r")
							position_1_id = @@pos_next_id
							game_moves_file.write("#{@@move_next_id}\t#{cur_index}\t#{@@move_next_id}\t#{game.id}\t#{i}\r")
							@@pos_next_id = @@pos_next_id + 1
							prev_index = cur_index

						end

					end

				end

				print "\rProcessing Game #{@@games_count+ind}"\

			end

			print "\nWriting games to file"
			tagged_games_file.close
			# File.write(@@games_temp_filename, new_games.map { |x| "#{x[:id]}\t#{x[:game_file_id]}\t#{x[:offset]}\t#{x[:length]}\t#{x[:result]}\t#{x[:eco]}\t#{x[:str_result]}\t#{x[:black]}\t#{x[:white]}\t#{x[:round]}\t#{x[:date]}\t#{x[:site]}\t#{x[:event]}\t#{x[:blackelo]}\t#{x[:whiteelo]}\t#{x[:opening]}\t#{x[:fen]}\t#{x[:setup]}\t#{x[:variation]}\t#{x[:year]}\t#{x[:year]}\t#{x[:month]}\t#{x[:day]}\t#{x[:eco_code]}\t#{x[:eco_num]}\r" }.join)
			print "\nWriting tags to file"
			File.write(@@tags_temp_filename, new_tags.values.map { |x| "#{x[:id]}\t#{x[:name]}\r" }.join)
			# print "\nWriting game tags to file"
			# File.write(@@game_tags_temp_filename, new_game_tags.map { |x| "#{x[:game_id]}\t#{x[:tag_id]}\t#{x[:tag_value]}\r" }.join)
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


		
		def self.process_big(filename)
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

					game = TaggedGame.new({id:@@game_next_id, game_file_id: gamefile.id, offset: @@lengths[ind],length: @@lengths[ind+1]-@@lengths[ind]})
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

					if @@known_positions[first_fen.to_short_s]
						position_1_fen = first_fen
						position_1_id = @@known_positions[first_fen.to_short_s].id
					else
						positions_file[first_fen.active][first_fen.to_position.figures_count].write("#{@@pos_next_id}\t#{first_fen.to_short_s}\r") 
						position_1_fen = first_fen
						position_1_id = @@pos_next_id
						@@pos_next_id = @@pos_next_id + 1
					end
					@@fen_lists[ind].each_with_index do |fn,i|

						if i>0

							if @@known_positions[fn.to_short_s]
								position_2_fen = fn
								position_2_id = @@known_positions[fn.to_short_s].id
							else
								positions_file[fn.active][fn.to_position.figures_count].write("#{@@pos_next_id}\t#{fn.to_short_s}\r")
								position_2_fen = fn
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
							moves_file[position_1_fen.active][position_1_fen.to_position.figures_count].write("#{@@move_next_id}\t#{position_1_id}\t#{position_2_id}\t#{position_2_fen.to_position.figures_count}_#{position_2_fen.active}\t#{gam.moves[i-1]}\r")
							game_moves_file.write("#{@@move_next_id}\t#{position_1_fen.to_position.figures_count}_#{position_1_fen.active}\t#{game.id}\t#{i}\r")

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

					game = TaggedGame.new({id:@@game_next_id, game_file_id: gamefile.id, offset: @@lengths[ind],length: @@lengths[ind+1]-@@lengths[ind]})
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
		def self.process_unique(filename)
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

					game = TaggedGame.new({id:@@game_next_id, game_file_id: gamefile.id, offset: @@lengths[ind],length: @@lengths[ind+1]-@@lengths[ind]})
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



end