require 'json'
require 'pp'
require 'net/http'
require 'uri'

uri = URI(ARGV[0])

loop do
  json = Net::HTTP.start(uri.host, uri.port) do |h|
    req = Net::HTTP::Get.new(uri.path)
    resp = h.request(req)

    if Net::HTTPSuccess === resp
      resp.body
    end
  end

  if json
    doc = JSON.parse(json)

    pp doc
    break
  end

  sleep 1
end
