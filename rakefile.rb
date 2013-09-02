task :default => [:parse]

task :loader do
  sh %{ruby -I . file_loader.rb}
end

task :assembler do
  sh %{ruby -I . chunk_assembler.rb}
end

PARSERS = 8

1.upto PARSERS do |i|
  task "parser#{i}".to_sym do
    sh %{ruby -I . parser_client.rb}
  end
end

def parser_list
  build_list = []
  1.upto PARSERS do |i|
    build_list << "parser#{i}".to_sym
  end
  build_list << :loader
  build_list << :assembler
end

@parser_list  = parser_list

desc "parse a bulk file"
multitask :parse => @parser_list do
  puts "done parsing"
end


