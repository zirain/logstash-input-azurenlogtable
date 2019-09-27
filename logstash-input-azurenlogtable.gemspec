Gem::Specification.new do |s|
  s.name          = 'logstash-input-azurenlogtable'
  s.version       = '0.1.1'
  s.licenses      = ['Apache-2.0']
  s.summary       = 'This plugin collects NLog data from Azure Storage Tables.'
  s.description   = 'This gem is a Logstash plugin. It reads and parses NLog data from Azure Storage Tables.'
  s.homepage      = 'https://github.com/zirain/logstash-input-azurenlogtable'
  s.authors       = ['zirain']
  s.email         = 'zirain2009@gmail.com'
  s.require_paths = ['lib']

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']
   # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "input" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", ">= 1.60", "<= 2.99"
  s.add_runtime_dependency "logstash-codec-plain"
  s.add_runtime_dependency 'azure-storage', '~> 0.13.0.preview'
  s.add_runtime_dependency 'stud', '~> 0.0', '>= 0.0.22'
  s.add_development_dependency 'logstash-devutils', '>= 1.1.0'
  s.add_development_dependency 'logging'
end
