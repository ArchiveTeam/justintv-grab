require 'csv'

items = []

class Item < Struct.new(:views, :page_url, :video_url)
end

threshold = ARGV[1].to_i || 100

CSV.foreach(ARGV[0], col_sep: ';', headers: true) do |row|
  items << Item.new(row['views_count'].to_i,
                    "http://www.justin.tv/#{row['login']}/b/#{row['id']}",
                    row['concat'])
end

items.sort_by(&:views).reverse.select { |i| i.views >= threshold }.each do |item|
  puts "#{item.views}\t#{item.video_url}\t#{item.page_url}"
end
