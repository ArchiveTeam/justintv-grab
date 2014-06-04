#!/usr/bin/env ruby

require 'celluloid'
require 'net/http/persistent'
require 'redis'
require 'connection_pool'
require 'uri'
require 'logger'
require 'nokogiri'
require 'json'
require 'pp'
require 'oauth'

require File.expand_path('../summary', __FILE__)

WORKERS = 2
RETRIES = 10

RP = ConnectionPool.new(size: WORKERS + 1) do
  Redis.new
end

HP = ConnectionPool.new(size: WORKERS) do
  Net::HTTP::Persistent.new 'justouttv'
end

OAUTH_CONSUMER_KEY = ENV['OAUTH_CONSUMER_KEY']
OAUTH_CONSUMER_SECRET = ENV['OAUTH_CONSUMER_SECRET']

if !OAUTH_CONSUMER_KEY || !OAUTH_CONSUMER_SECRET
  abort "OAUTH_CONSUMER_KEY or OAUTH_CONSUMER_SECRET not defined"
end

class Worker
  include Celluloid
  include Celluloid::Logger

  def discover(channel_api_uri)
    # If we've already seen this channel URI, return the cached data.
    if (summaries = cached_data(channel_api_uri))
      return summaries
    end

    # Otherwise, get all the channel JSON.
    json = channel_json(channel_api_uri)
    return unless json

    # For each archive, build a [archive, page URI] pair.
    uris = page_uris(json, channel_api_uri)
    return unless uris

    pairs = json.zip(uris)

    # Visit each page, extract the view count.
    summaries = pairs.map do |obj, page_uri|
      views = view_count(page_uri)

      if !views
        error "Failed to retrieve view count for #{page_uri}"
      end

      Summary.new(views, obj, page_uri)
    end

    # Store it.
    r { |c| c.set(channel_api_uri, summaries.to_json) }
    summaries
  end

  private

  def cached_data(uri)
    data = r { |c| c.get(uri) }

    if data
      JSON.parse(data).map { |d| Summary.from_json(d) }
    end
  end

  def channel_json(uri)
    oauth_get_with_retry(uri) do |resp|
      case resp
      when Net::HTTPSuccess; JSON.parse(resp.body)
      when Net::HTTPNotFound; []
      when Net::HTTPBadRequest then
        error "GET #{uri} #{resp.code}: rate limit exceeded; sleeping for 300 sec"
        sleep 300
        nil
      end
    end.tap { sleep (WORKERS + (rand * 5)) }
  end

  def page_uris(json, channel_api_uri)
    json.map do |obj|
      channel_name = if obj['stream_name'] && !obj['stream_name'].strip.empty?
                       obj['stream_name'].sub(/.+_user_/, '')
                     else
                       channel_api_uri.path.split('/').last.sub('.json', '').tap do |guess|
                         warn "Unable to find stream name in JSON for #{channel_api_uri}; guessing #{guess}"
                       end
                     end

      broadcast_id = obj['id']
      URI("http://www.justin.tv/#{channel_name}/b/#{broadcast_id}")
    end
  end

  def view_count(page_uri)
    body = get_with_retry(page_uri) do |resp|
      case resp
      when Net::HTTPSuccess; resp.body

      # As far as I can tell, this actually means "page not found", so we'll
      # just roll with it
      when Net::HTTPRedirection; :redirected
      end
    end

    if body == :redirected
      nil
    else
      doc = Nokogiri.HTML(body)
      (doc/'#archive_views_count').text.to_i
    end
  end

  def oauth_get_with_retry(uri)
    retries = 0

    while retries < RETRIES
      consumer = OAuth::Consumer.new(OAUTH_CONSUMER_KEY,
                                     OAUTH_CONSUMER_SECRET,
                                     site: 'http://api.justin.tv',
                                     http_method: 'get')

      access_token = OAuth::AccessToken.new(consumer)
      resp = access_token.get(uri.path)
      ret = yield resp

      if ret
        info "OAuth GET #{uri} #{resp.code}"
        return ret
      else
        retries += 1
        sleep_time = 1.5 ** retries
        info "OAuth GET #{uri} #{resp.code}: failed, sleeping #{sleep_time} sec before next retry"
        sleep sleep_time
      end
    end
  end

  def get_with_retry(uri)
    retries = 0

    while retries < RETRIES
      h do |c|
        req = Net::HTTP::Get.new(uri.path)
        resp = c.request uri, req

        ret = yield resp

        if ret
          info "GET #{uri} #{resp.code}"
          return ret
        else
          retries += 1
          sleep_time = 1.5 ** retries
          info "GET #{uri} #{resp.code}: failed, sleeping #{sleep_time} sec before next retry"
          sleep sleep_time
        end
      end
    end
  end

  def h
    HP.with { |c| yield c }
  end

  def r
    RP.with { |c| yield c }
  end
end

WP = Worker.pool(size: WORKERS)

# ---------------------------------------------------------------------------

active = []
log = Logger.new($stderr)

loop do
  status = WORKERS.times do
    item = RP.with do |c|
      it = c.rpoplpush('pending', 'working')
      c.srem('pending-set', it)
      c.sadd('working-set', it)
      it
    end

    break :no_item unless item

    active << [item, WP.future(:discover, URI(item))]
  end

  active.each do |l, future|
    summaries = future.value

    if !summaries
      log.error "#{l} failed to resolve"
      next
    end

    summaries.each do |s|
      $stdout.puts "#{s.views}\t#{s.archive_video_file}\t#{s.page_uri}"
    end

    RP.with do |c|
      c.srem('working-set', l)
      c.sadd('done-set', l)
      c.lrem('working', 1, l)
    end
  end

  active.clear

  if status == :no_item
    break
  end
end
