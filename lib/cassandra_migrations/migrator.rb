# encoding: utf-8

module CassandraMigrations
  module Migrator

    def self.up_to_latest!
      executed_migrations = read_executed_migrations

      new_migrations = get_all_migration_names.sort.reject do |migration_name|
        executed_migrations.include?(get_version_from_migration_name(migration_name))
      end

      unless new_migrations.empty?
        new_migrations.each { |migration| up(migration) }
      end

      new_migrations.size
    end

    def self.rollback!(count=1)
      current_version = read_current_version

      executed_migrations = get_all_migration_names.sort.reverse.select do |migration_name|
        get_version_from_migration_name(migration_name) <= current_version
      end

      down_count = 0

      unless executed_migrations.empty?
        count.times do |i|
          if executed_migrations[i]
            down(executed_migrations[i], executed_migrations[i+1])
            down_count += 1
          end
        end
      end

      down_count
    end

    def self.read_current_version
      read_executed_migrations.last || 0
      #
      # That's the old single-highest-version code:
      #
      # begin
      #   Cassandra.select('cassandra_migrations_metadata', :selection => "data_name='version'", :projection => 'data_value').first['data_value'].to_i
      # rescue ::Cassandra::Errors::InvalidError => e # table cassandra_migrations_metadata does not exist
      #   Cassandra.execute("CREATE TABLE cassandra_migrations_metadata (data_name varchar PRIMARY KEY, data_value varchar)")
      #   Cassandra.write!('cassandra_migrations_metadata', {:data_name => 'version', :data_value => '0'})
      #   return 0
      # end
    end

    private

    def self.read_executed_migrations
      begin
        Cassandra.select('schema_migrations', :selection => "type='migration'", :projection => 'version').map { |a| a['version'].to_i }
      rescue ::Cassandra::Errors::InvalidError
        Cassandra.execute <<~CQL
          CREATE TABLE schema_migrations (type TEXT, version TEXT, PRIMARY KEY (type, version))
          WITH CLUSTERING ORDER BY (version ASC);
        CQL

        r = []

        # Insert all the versions that had been executed in the old way
        old = old_single_highest_version
        if old > 0
          r = get_all_migration_versions.select { |version| version <= old }

          r.each do |version|
            Cassandra.write!('schema_migrations', :type => 'migration', :version => version.to_s)
          end

          Cassandra.execute('DROP TABLE cassandra_migrations_metadata')
        end

        return r
      end
    end

    def self.old_single_highest_version
      begin
        Cassandra.select('cassandra_migrations_metadata', :selection => "data_name='version'", :projection => 'data_value').first['data_value'].to_i
      rescue ::Cassandra::Errors::InvalidError => e # table cassandra_migrations_metadata does not exist
        return 0
      end
    end

    def self.up(migration_name)
      # load migration
      require migration_name
      # run migration
      get_class_from_migration_name(migration_name).new.migrate(:up)

      # update version
      #Cassandra.write!('cassandra_migrations_metadata', {:data_name => 'version', :data_value => get_version_from_migration_name(migration_name).to_s})
      Cassandra.write!('schema_migrations', :type => 'migration', :version => get_version_from_migration_name(migration_name).to_s)
    end

    def self.down(migration_name, previous_migration_name=nil)
      # load migration
      require migration_name
      # run migration
      get_class_from_migration_name(migration_name).new.migrate(:down)

      # downgrade version
      # if previous_migration_name
      #   Cassandra.write!('cassandra_migrations_metadata', {:data_name => 'version', :data_value => get_version_from_migration_name(previous_migration_name).to_s})
      # else
      #   Cassandra.write!('cassandra_migrations_metadata', {:data_name => 'version', :data_value => '0'})
      # end
      Cassandra.delete!('schema_migrations', "type='migration' AND version='#{get_version_from_migration_name(migration_name)}'")
    end

    def self.get_all_migration_names
      Dir[Rails.root.join("db", "cassandra_migrate/[0-9]*_*.rb")]
    end

    def self.get_all_migration_versions
      get_all_migration_names.sort.map { |migration_name| get_version_from_migration_name(migration_name) }
    end

    def self.get_class_from_migration_name(filename)
      migration_name = filename.match(/[0-9]+_(.+)\.rb$/).captures.first.camelize
      migration_name.constantize
    rescue NameError => e
      raise Errors::MigrationNamingError, "Migration file names must match the class name in the migration—could not find class #{migration_name}."
    end

    def self.get_version_from_migration_name(filename)
      filename.match(/\/([0-9]+)_.+\.rb$/).captures.first.to_i
    rescue
      raise Errors::MigrationNamingError, "Migration file names must start with a numeric version prefix."
    end
  end
end
