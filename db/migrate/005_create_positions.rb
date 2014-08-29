#encoding utf-8

class CreatePositions < ActiveRecord::Migration

	# def self.up
	# 	(2..32).each do |n|
	# 		['w', 'b'].each do |color|
	# 			create_table "positions_#{n}_#{color}" do |t|
	# 				t.string :fen
	# 			end
	# 		end
	# 	end

	# end

	# def self.down
	# 	(2..32).each do |n|
	# 		['w','b'].each do |color|
	# 			drop_table "positions_#{n}_#{color}"
	# 		end
	# 	end
	# end

	def self.up
		create_table "positions" do |t|
			t.string :fen
		end
	end

	def self.down
		drop_table "positions"
	end
	
end