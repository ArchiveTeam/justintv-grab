require 'redis'
require 'json'

require File.expand_path('../summary', __FILE__)

r = Redis.new

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
    print summaries.length
    print '...'
  end
end

print summaries.length
puts
puts

summaries.sort_by(&:views).reverse.each do |summary|
  video_url = summary.archive_video_file

  puts "#{summary.views.to_i}\t#{video_url}\t#{summary.page_uri}"
end
