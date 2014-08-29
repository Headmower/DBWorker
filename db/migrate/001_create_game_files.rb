#encoding utf-8

class CreateGameFiles < ActiveRecord::Migration
	def self.up
		create_table :game_files do |t|
			t.string :file_path
		end
	end

	def self.down
		drop_table :game_files
	end
end