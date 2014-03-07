#encoding: utf-8

require 'yaml'

task :default => [:parse]


task :loader do
  verbose(false) do
    sh %{ruby -I . file_loader.rb}
  end
end

task :emitter do
  verbose(false) do
    sh %{ruby -I . emitter.rb}
  end
end

config = YAML.load_file("config.yaml")
PARSERS = config[:clients].to_i

1.upto PARSERS do |i|
  task "parser#{i}".to_sym do
    verbose(false) do
      sh %{ruby -I . parser_client.rb}
    end
  end
end

def parser_list
  build_list = []
  build_list << :emitter
  build_list << :loader
  1.upto PARSERS do |i|
    build_list << "parser#{i}".to_sym
  end
  build_list
end

@parser_list  = parser_list

desc "parse a bulk file"
multitask :parse => @parser_list do
  puts "done parsing"
end

