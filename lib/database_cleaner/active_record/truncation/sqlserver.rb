require 'tsort'

module DatabaseCleaner
  module ConnectionAdapters
    module SQLServerAdapter
      class Cleaner
        def initialize(connection)
          @con = connection
          @schema = DatabaseSchema.new(@con)
        end

        def truncate_tables(tables)
          foreign_keys = @schema.foreign_keys

          targeted_tables, not_targeted_tables = tables.partition { |t| foreign_keys.key?(t) }

          not_targeted_tables.each { |t| truncate_table_by_truncating(t) }

          OrderedDeletion.new(targeted_tables, foreign_keys).each_strongly_connected_component do |c|
            disable_foreign_keys_within_component(c) do
              c.each { |t| truncate_table_by_deleting(t) }
            end
          end
        end

        def pre_count_truncate_tables(tables)
          db = DatabaseState.new(@con)
          truncate_tables(tables.select { |t| db.has_been_used?(t) })
        end

        class OrderedDeletion
          include TSort

          def initialize(tables, foreign_keys)
            @tables = tables
            @foreign_keys = foreign_keys
          end

          def tsort_each_node(&block)
            @tables.each(&block)
          end

          def tsort_each_child(node, &block)
            children = @foreign_keys[node].to_a & @tables
            children.each(&block)
          end
        end

        private

        def disable_foreign_keys_within_component(tables)
          if tables.length <= 1
            yield
          else
            @schema.disable_foreign_keys(tables) do
              yield
            end
          end
        end

        def truncate_table_by_truncating(table_name)
          @con.execute("TRUNCATE TABLE #{@con.quote_table_name(table_name)}")
        end

        def truncate_table_by_deleting(table_name)
          @con.execute("DELETE FROM #{@con.quote_table_name(table_name)}")

          if reseed = @schema.reseeds[table_name]
            @con.execute("DBCC CHECKIDENT(#{@con.quote_table_name(table_name)}, RESEED, #{reseed}) WITH NO_INFOMSGS")
          end
        end
      end

      class DatabaseSchema
        def initialize(connection)
          @con = connection
        end

        def disable_foreign_keys(tables)
          begin
            tables.each { |t| @con.execute "ALTER TABLE #{@con.quote_table_name(t)} NOCHECK CONSTRAINT ALL" }
            yield
          ensure
            tables.each { |t| @con.execute "ALTER TABLE #{@con.quote_table_name(t)} CHECK CONSTRAINT ALL" }
          end
        end

        def reseeds
          # FIXME: permanent caching should depend on cache_tables option
          @reseeds ||= @con.select_rows(<<-SQL).to_h
            SELECT t.name, CAST(c.seed_value AS int) - CAST(c.increment_value AS int)
            FROM sys.identity_columns c
            JOIN sys.tables t ON t.object_id = c.object_id
          SQL
        end

        def foreign_keys
          # FIXME: caching should depend on cache_tables option
          @foreign_keys ||=
            begin
              foreign_keys = @con.select_rows(<<-SQL)
                SELECT DISTINCT t_source.name source, t_target.name target
                FROM sys.foreign_keys fk
                JOIN sys.tables t_source ON t_source.object_id = fk.parent_object_id
                JOIN sys.tables t_target ON t_target.object_id = fk.referenced_object_id
              SQL

              foreign_keys.each_with_object({}) do |(source_table, target_table), hash|
                (hash[target_table] ||= []).push(source_table)
              end
            end
        end
      end

      class DatabaseState
        def initialize(connection)
          @con = connection
        end

        def has_been_used?(table)
          identity_changed = used_identities[table] # three-valued logic (nil, true, false)
          identity_changed.nil? ? has_rows?(table) : identity_changed
        end

        private

        def has_rows?(table)
          @con.select_value("SELECT CAST(1 as BIT) WHERE EXISTS (SELECT 1 FROM #{@con.quote_table_name(table)})")
        end

        def used_identities
          @used_identities ||= @con.select_rows(<<-SQL).to_h
            SELECT t.name, CAST(CASE WHEN c.last_value >= c.seed_value THEN 1 ELSE 0 END AS BIT)
            FROM sys.identity_columns c
            JOIN sys.tables t ON c.object_id = t.object_id
          SQL
        end
      end
    end
  end
end
