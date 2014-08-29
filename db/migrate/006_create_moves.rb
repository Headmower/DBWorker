#encoding utf-8

class CreateMoves < ActiveRecord::Migration
	# def self.up
	# 	(2..32).each do |n|
	# 		['w','b'].each do |color|
	# 			create_table "moves_#{n}_#{color}" do |t|
	# 				t.string :text
	# 				t.string :to_table_id
	# 				t.references :from
	# 				t.references :to
	# 			end
	# 		end
	# 	end
	# end

	# def self.down
	# 	(2..32).each do |n|
	# 		['w','b'].each do |color|
	# 			drop_table "moves_#{n}_#{color}"
	# 		end
	# 	end
	# end


	def self.up
		create_table "moves" do |t|
			t.string :text
			t.string :to_table_id
			t.references :from
			t.references :to
		end
	end

	def self.down
		drop_table "moves"
	end
end