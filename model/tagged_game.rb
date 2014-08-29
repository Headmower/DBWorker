class TaggedGame < ActiveRecord::Base
	
	belongs_to :game_file
end