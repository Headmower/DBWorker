#encoding utf-8

class CreateGameTags < ActiveRecord::Migration
	def self.up
		create_table :game_tags do |t|
			t.references :game
			t.references :tag
			t.string :tag_value
		end
	end

	def self.down
		drop_table :game_tags
	end
end