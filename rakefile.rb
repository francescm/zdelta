task :default => [:parse]

PARSERS = 8

task :conf do
  ENV['CLIENTS'] = PARSERS.to_s
  ENV['OLD_FILE'] = "live.ldif"
  ENV['NEW_FILE'] = "single.ldif"
  ENV['LOADER_SOCKET'] = "ipc://loader.ipc"
  ENV['CATALOG_SOCKET'] = "ipc://emitter.ipc"
end

task :loader => :conf do
  sh %{ruby -I . file_loader.rb}
end

task :emitter => :conf do
  sh %{ruby -I . emitter.rb}
end


1.upto PARSERS do |i|
  task "parser#{i}".to_sym => :conf do
    sh %{ruby -I . parser_client.rb}
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

