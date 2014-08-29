module DBWorker

		#Метод, парсящий игры
		def self.parse(games_text, step = 60000)
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

			shitty_games_file.close
			file = nil
			GC.enable
			GC.start
			return (Time.now - start_time)

	end

end