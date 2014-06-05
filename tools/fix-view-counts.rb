require 'celluloid'
require 'net/http/persistent'
require 'redis'
require 'connection_pool'
require 'nokogiri'
require 'logger'
require 'json'

require File.expand_path('../summary', __FILE__)

WORKERS = 4
RETRIES = 10

HP = ConnectionPool.new(size: WORKERS) do
  Net::HTTP::Persistent.new 'justouttv'
end

RP = ConnectionPool.new(size: WORKERS + 1) do
  Redis.new
end

class Worker
  include Celluloid
  include Celluloid::Logger

  def run(item)
    # Get the item's summaries.
    json = RP.with { |c| c.get(item) }
    doc = JSON.parse(json)
    summaries = doc.map { |d| Summary.from_json(d) }

    summaries.each do |s|
      next if !s.page_uri

      # Get the page.
      body = get_with_retry(URI(s.page_uri)) do |resp|
        case resp
        when Net::HTTPSuccess; resp.body
        end
      end

      # Extract the view count and leave it as a string.
      doc = Nokogiri.HTML(body)
      vc = (doc/'#archive_views_count').text

      if s.views.to_s != vc
        info "#{s.page_uri}: Old VC: #{s.views}, new VC: #{vc}"
      end

      s.views = vc
    end

    RP.with { |c| c.set item, summaries.to_json }
  end

  private

  def get_with_retry(uri, &block)
    retries = 0

    while retries < RETRIES
      HP.with do |c|
        req = Net::HTTP::Get.new(uri.path)
        resp = c.request uri, req

        if resp.kind_of?(Net::HTTPRedirection)
          new_uri = URI(redirect_url(resp))
          debug "#{resp.code}: #{uri} -> #{new_uri}"
          return get_with_retry(new_uri, &block)
        else
          ret = yield resp

          if ret
            return ret
          else
            retries += 1
            sleep_time = 1.5 ** retries
            warn "GET #{uri} #{resp.code}: failed, sleeping #{sleep_time} sec before next retry"
            sleep sleep_time
          end
        end
      end
    end
  end

  def redirect_url(resp)
    if resp['location'].nil?
      resp.body.match(/<a href=\"([^>]+)\">/i)[1]
    else
      resp['location']
    end
  end
end

WP = Worker.pool(size: WORKERS)

cursor = 0

loop do
  tasks = []

  RP.with do |r|
    cursor, results = r.scan(cursor, count: 500, match: 'http*')

    if cursor.to_i == 0
      break
    else
      results.each do |r|
        tasks << WP.future(:run, r)
      end
    end

    tasks.each do |t|
      t.value
    end
  end
end
