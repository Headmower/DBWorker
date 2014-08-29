#encoding utf-8

class CreateGameMoves < ActiveRecord::Migration
	def self.up
		table_name = 'game_moves'
		create_table :game_moves do |t|
			t.string :table_index
			t.references :move
			t.references :game
			t.integer :half_move_number
		end

		breed(table_name)
		create_insert_rules(table_name)
	end

	def self.down
		drop_table :game_moves
		genocide(table_name)
	end
end