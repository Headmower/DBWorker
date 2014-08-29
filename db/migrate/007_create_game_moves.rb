#encoding utf-8

class CreateGameMoves < ActiveRecord::Migration
	def self.up
		create_table :game_moves do |t|
			t.references :move
			t.references :game
			t.integer :half_move_number
			t.string :move_table_id
		end
	end

	def self.down
		drop_table :game_moves
	end
end