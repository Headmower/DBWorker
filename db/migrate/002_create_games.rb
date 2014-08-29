#encoding utf-8

class CreateGames < ActiveRecord::Migration
	def self.up
		create_table :games do |t|
			t.references :game_file
			t.integer :offset
			t.integer :length
			t.integer :result
		end
	end

	def self.down
		drop_table :games
	end
end