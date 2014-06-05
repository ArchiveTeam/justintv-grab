require 'redis'
require 'json'

require File.expand_path('../summary', __FILE__)

r = Redis.new(url: 'redis://localhost:6380')

threshold = ARGV[0].to_i

cursor = 0
summaries = []

loop do
  cursor, results = r.scan(cursor, count: 100, match: 'http*')

  if cursor.to_i == 0
    break
  else
    jsons = r.mget(results)
    jsons.each do |json|
      doc = JSON.parse(json)

      doc.each do |obj|
        summaries << Summary.from_json(obj)
      end
    end
  end

  if summaries.length % 100 == 0
    $stderr.print summaries.length
    $stderr.print '...'
  end
end

$stderr.print summaries.length
$stderr.puts
$stderr.puts

summaries.sort_by { |s| s.views_num }.select { |s| s.views_num >= threshold }.reverse.each do |summary|
  video_url = summary.archive_video_file

  puts "#{summary.views_num}\t#{video_url}\t#{summary.page_uri}"
end
