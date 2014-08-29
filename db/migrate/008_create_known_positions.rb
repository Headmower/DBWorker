#encoding utf-8

class CreateKnownPositions < ActiveRecord::Migration
	def self.up
		create_table :known_positions do |t|
			t.string :fen
			t.string :belong_code
		end

	end

	def self.down
		drop_table :known_positions
	end
end