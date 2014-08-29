class Move < ActiveRecord::Base
	
	belongs_to :from, class_name: :Position
	belongs_to :to, class_name: :Position
	has_many :game_moves


	def self.max_id()

		maxid = 0
		(2..32).each do |i|
			['w','b'].each do |c|
				self.table_name = "moves_#{i}_#{c}"
				maxid = self.last.try(:id) if (self.last.try(:id).to_i >= maxid.to_i)
			end
		end

		return maxid
	end
end