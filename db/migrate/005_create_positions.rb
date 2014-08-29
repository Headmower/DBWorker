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
		table_name = 'positions'
		create_table :positions do |t|
			t.string :table_index
			t.string :fen
			t.integer :played
			t.integer :white_victory_count
			t.integer :black_victory_count
			t.integer :draw_count
		end

		breed(table_name)
		create_insert_rules(table_name)
	end

	def self.down
		drop_table :positions
		genocide(table_name )
	end
	
end