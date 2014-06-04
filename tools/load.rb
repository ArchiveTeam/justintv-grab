require 'redis'

r = Redis.new

puts "Deduplicating"

data = File.read(ARGV[0]).split("\n").shuffle

data.reject!.with_index do |d, i|
  if i % 1000 == 0
    print "#{i}..."
  end

  %w(pending-set working-set done-set).any? do |s|
    r.sismember(s, d)
  end
end

puts
puts "Loading"

data.shuffle.each.with_index do |l, i|
  r.pipelined do
    r.lpush 'pending', l
    r.sadd 'pending-set', l

    if i % 1000 == 0
      print "#{i}..."
    end
  end
end

puts
puts "Loaded #{data.length} items"
