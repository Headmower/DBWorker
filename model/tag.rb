class Tag < ActiveRecord::Base
	
	has_many :game_tags
end