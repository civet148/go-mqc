package types

const (
	ContentEncoding_UTF8      = "utf-8"
	ContentEncoding_GB2312    = "gb2312"
	ContentEncoding_GB18030   = "gb18030"
	ContentEncoding_GBK       = "gbk"
	ContentEncoding_ASCII     = "ascii"
	ContentEncoding_UTF16     = "utf-16"
	ContentEncoding_UTF16BE   = "utf-16be"
	ContentEncoding_UTF16LE   = "utf-16le"
	ContentEncoding_UTF32     = "utf-32"
	ContentEncoding_UTF32BE   = "utf-32be"
	ContentEncoding_UTF32LE   = "utf-32le"
	ContentEncoding_Unicode   = "unicode"
	ContentEncoding_UnicodeLE = "unicode-le"
	ContentEncoding_UnicodeBE = "unicode-be"
)

const (
	ContentType_ApplicationJSON = "application/json"         //	JSON 数据	最常用。RabbitMQ 官方社区也推荐对 JSON 编码的消息使用此类型，它能让消费者明确知道该用 JSON 解析器处理消息体。
	ContentType_TextPlain       = "text/plain"               //	纯文本	文本文件的默认类型，适用于人类可读、不含二进制数据的简单文本。
	ContentType_OctetStream     = "application/octet-stream" //	任意二进制数据	通用后备类型。当消息体是未知格式、或明确是二进制数据（如图片、Protobuf 序列化后的数据）时使用。
	ContentType_ApplicationXML  = "application/xml"          //	XML 数据	用于 XML 格式的消息体。Spring AMQP 等框架在未指定类型时甚至会默认假设为 XML。
	ContentType_TextXML         = "text/xml"                 //	XML 数据	用于 XML 格式的消息体。Spring AMQP 等框架在未指定类型时甚至会默认假设为 XML。
	ContentType_TextH           = "text/html"                // HTML 文档	包含 HTML 标记的文本数据。
	ContentType_ApplicationYAML = "application/x-yaml"       //	YAML 数据	YAML 格式的配置或数据。
	ContentType_ImageJPEG       = "image/jpeg"               //	图片数据	当消息体是图片文件时使用。
	ContentType_ImagePNG        = "image/png"                //	图片数据	当消息体是图片文件时使用。
)
