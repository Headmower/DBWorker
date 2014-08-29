class GameFile < ActiveRecord::Base

	has_many :tagged_games
end
