# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = "fluent-plugin-mongo2"
  spec.version       = "0.0.1"
  spec.authors       = ["Hiroshi Hatake"]
  spec.email         = ["hatake@clear-code.com"]
  spec.license       = "Apache-2.0"

  spec.summary       = %q{A Fluentd plugin uses MongoDB ruby driver 2.x series.}
  spec.description   = %q{A Fluentd plugin uses MongoDB ruby driver 2.x series.}
  spec.homepage      = "https://github.com/cosmo0920/fluent-plugin-mongo2"

  # Prevent pushing this gem to RubyGems.org by setting 'allowed_push_host', or
  # delete this section to allow pushing this gem to any host.
  if spec.respond_to?(:metadata)
    spec.metadata['allowed_push_host'] = "https://rubygems.org"
  else
    raise "RubyGems 2.0 or newer is required to protect against public gem pushes."
  end

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.10"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "test-unit", "~> 3.1"
  spec.add_runtime_dependency "fluentd", [">= 0.12.0", "< 0.14.0"]
  spec.add_runtime_dependency "bson_ext"
  spec.add_runtime_dependency "mongo", "~> 2.2.0"
end
