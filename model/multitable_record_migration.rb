class ActiveRecord::Migration

	def self.create_insert_trigger(table_name)

		ActiveRecord::Base.connection.execute <<-eos
				CREATE OR REPLACE FUNCTION #{table_name}_insert_trigger()
				RETURNS TRIGGER AS $$
				BEGIN
				    IF ( NEW.logdate >= DATE '2006-02-01' AND
				         NEW.logdate < DATE '2006-03-01' ) THEN
				        INSERT INTO measurement_y2006m02 VALUES (NEW.*);
				    ELSIF ( NEW.logdate >= DATE '2006-03-01' AND
				            NEW.logdate < DATE '2006-04-01' ) THEN
				        INSERT INTO measurement_y2006m03 VALUES (NEW.*);
				    ELSIF ( NEW.logdate >= DATE '2008-01-01' AND
				            NEW.logdate < DATE '2008-02-01' ) THEN
				        INSERT INTO measurement_y2008m01 VALUES (NEW.*);
				    ELSE
				        RAISE EXCEPTION \'Table index does not exist. Give right table_index or fix the #{table_name}_insert_trigger() function!\';
				    END IF;
				    RETURN NULL;
				END;
				$$
				LANGUAGE plpgsql;

				CREATE TRIGGER #{table_name}_insert_trigger
				    BEFORE INSERT ON #{table_name}
				    FOR EACH ROW EXECUTE PROCEDURE #{table_name}_insert_trigger();
		eos

	end

	def self.create_insert_rules(table_name)
		(2..32).each do |num|
			['w','b'].each do |clr|
				puts "Creating INSERT rule for #{table_name}_#{num}_#{clr}"
				ActiveRecord::Base.connection.execute <<-eos
						CREATE OR REPLACE RULE #{table_name}_#{num}_#{clr}insert AS
						   ON INSERT TO #{table_name}
						   WHERE NEW.table_index = \'#{num}_#{clr}\'
						   DO INSTEAD
						   
						INSERT INTO #{table_name}_#{num}_#{clr} VALUES (NEW.*);
				eos
			end
		end
	end

	def self.breed(table_name)

		(2..32).each do |num|
			['w','b'].each do |clr|
				puts "Creating #{table_name}_#{num}_#{clr}"
				creator_sql = <<-eos
					CREATE TABLE #{table_name}_#{num}_#{clr}
					(
					  CONSTRAINT #{table_name}_#{num}_#{clr}_table_index_check CHECK (table_index::text = \'#{num}_#{clr}\'::text)
					)
					INHERITS (#{table_name})
					WITH (
					  OIDS=FALSE
					);
				eos
				ActiveRecord::Base.connection.execute creator_sql
			end
		end

	end


	def self.genocide(table_name)

		(2..32).each do |num|
			['w','b'].each do |clr|
				puts "Destroying #{table_name}_#{num}_#{clr}"
				destructor_sql = "DROP TABLE #{table_name}_#{num}_#{clr}"
				ActiveRecord::Base.connection.execute destructor_sql
			end
		end

	end
end