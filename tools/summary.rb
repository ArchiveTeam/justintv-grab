class Summary < Struct.new(:views, :archive_object, :page_uri)
  def self.from_json(json)
    new(json['views'],
        json['archive_object'],
        json['page_uri'])
  end

  def archive_video_file
    archive_object['video_file_url']
  end

  def views_num
    views.to_i
  end

  def to_json(*args)
    { views: views,
      archive_object: archive_object,
      page_uri: page_uri
    }.to_json(*args)
  end
end
