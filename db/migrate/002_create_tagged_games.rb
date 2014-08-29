#encoding utf-8

class CreateTaggedGames < ActiveRecord::Migration
	def self.up
		create_table :tagged_games do |t|
			t.references :game_file
			t.integer :offset
			t.integer :length
			t.integer :result
			t.integer :blackelo
			t.integer :whiteelo
			t.integer :year
			t.integer :month
			t.integer :day
			t.integer :eco_num
			t.string :eco
			t.string :str_result
			t.string :black
			t.string :white
			t.string :round
			t.string :date
			t.string :site
			t.string :event
			t.string :opening
			t.string :fen
			t.string :setup
			t.string :variation
			t.column :eco_code, 'char(1)'
			t.column :fts, 'tsvector'
		end
	end

	def self.down
		drop_table :tagged_games
	end
end