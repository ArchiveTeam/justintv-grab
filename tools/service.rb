require 'redis'

r = Redis.new(url: 'redis://localhost:6381/0')

r.pipelined do
  $stdin.each_line do |line|
    vc, vid_url, page_url = line.split("\t")
    item = page_url.sub('http://www.justin.tv/', '').chomp
    
    r.set item, vid_url
  end
end
