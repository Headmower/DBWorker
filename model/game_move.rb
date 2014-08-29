class GameMove < ActiveRecord::Base
	
	belongs_to :move
	belongs_to :tagged_game
end