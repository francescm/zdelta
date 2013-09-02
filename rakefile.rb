task :default => [:parse]

PARSERS = 8

task :conf do
  ENV['CLIENTS'] = PARSERS.to_s
  ENV['DATA_FILE'] = "bulk.ldif"
#  ENV['DATA_FILE'] = "registered.ldif"
#  ENV['DATA_FILE'] = "users.ldif"
  ENV['LOADER_SOCKET'] = "ipc://loader.ipc"
  ENV['CATALOG_SOCKET'] = "ipc://assembler.ipc"
end

task :loader => :conf do
  sh %{ruby -I . file_loader.rb}
end

task :assembler => :conf do
  sh %{ruby -I . chunk_assembler.rb}
end


1.upto PARSERS do |i|
  task "parser#{i}".to_sym => :conf do
    sh %{ruby -I . parser_client.rb}
  end
end

def parser_list
  build_list = []
  build_list << :assembler
  1.upto PARSERS do |i|
    build_list << "parser#{i}".to_sym
  end
  build_list << :loader
end

@parser_list  = parser_list

desc "parse a bulk file"
multitask :parse => @parser_list do
  puts "done parsing"
end

