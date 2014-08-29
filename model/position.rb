class Position < ActiveRecord::Base
	
	has_many :from_moves, class_name: :Move, foreign_key: :from
	has_many :to_moves, class_name: :Move, foreign_key: :to

	def self.max_id()
		maxid = 0
		(2..32).each do |i|
			['w','b'].each do |c|
				self.table_name = "positions_#{i}_#{c}"
				maxid = self.last.try(:id).to_i if (self.last.try(:id).to_i > maxid)
			end
		end

		return maxid
	end


end