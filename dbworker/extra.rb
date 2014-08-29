module DBWorker

		
	def self.same_figures_positions(filename, step = 2000000)
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


	def self.init_known_positions()
		belong_code = ""
		fn = PGN::FEN.new
		KnownPosition.all.select('id,fen').each do |x|
			fn = PGN::FEN.from_short_s(x.fen)
			belong_code = "#{fn.to_position.figures_count}_#{fn.active}"
			ActiveRecord::Base.connection.execute "UPDATE known_positions SET belong_code = '#{belong_code}' WHERE id = #{x.id}"

		end
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