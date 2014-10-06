Gem::Specification.new do |s|
  s.name        = 'cassandra_migrations'
  s.version     = '0.1.0'
  s.date        = '2014-06-18'
  s.license     = 'MIT'
  s.summary     = 'Cassandra schema management for a multi-environment developer.'
  s.description = 'A gem to manage Cassandra database schema for Rails. This gem offers migrations and environment specific databases out-of-the-box for Rails users.'
  s.authors     = ['Henrique Gubert', 'Brian Sam-Bodden']
  s.email       = ['guberthenrique@hotmail.com', 'bsbodden@integrallis.com']
  s.homepage    = 'https://github.com/hsgubert/cassandra_migrations'
  s.require_path = 'lib'
  s.required_rubygems_version = '>= 1.8.0'

  # s.files: The files included in the gem.
  s.files = Dir['lib/**/*', 'template/**/*']

  # s.test_files: Files that are used for testing the gem.
  s.test_files = Dir['s/**/*_s.rb']

  # s.executables: Executables that comes with the gem
  s.executables = ['prepare_for_cassandra']

  # s.add_dependency: Production dependencies
  s.add_dependency 'cassandra-driver', '1.0.0.beta.2'
  s.add_dependency 'rake', '~> 10'
  s.add_dependency 'rails', '>= 3.2'
  s.add_dependency 'colorize', '~> 0.7.3'

  # s.add_development_dependency: Development dependencies
  s.add_development_dependency 'rspec', '~> 2.99'
  s.add_development_dependency 'byebug', '~> 3.5.1'
  s.add_development_dependency 'bundler', '~> 1.3'
  s.add_development_dependency 'simplecov', '~> 0.8'
  s.add_development_dependency 'coveralls', '~> 0.7'
end
