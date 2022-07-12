from google.cloud import language_v1


# 1. Enable Cloud Natural Language API (** Needs billing **)
# 2. Create API Key (if needed)


class SentimentAnalysisConnector:
    def __init__(self):
        self.client = language_v1.LanguageServiceClient()

    def analyze(self, text_content):
        type_ = language_v1.Document.Type.PLAIN_TEXT
        document = {"content": text_content, "type_": type_}

        # Available values: NONE, UTF8, UTF16, UTF32
        encoding_type = language_v1.EncodingType.UTF8

        response = self.client.analyze_sentiment(request={'document': document, 'encoding_type': encoding_type})
        return response

    def sample_analyze_entity_sentiment(self, text_content):
        """
        Analyzing Entity Sentiment in a String

        Args:
          text_content The text content to analyze
        """

        # Available types: PLAIN_TEXT, HTML
        type_ = language_v1.types.Document.Type.PLAIN_TEXT

        # Optional. If not specified, the language is automatically detected.
        # For list of supported languages:
        # https://cloud.google.com/natural-language/docs/languages
        language = "en"
        document = {"content": text_content, "type_": type_, "language": language}

        # Available values: NONE, UTF8, UTF16, UTF32
        encoding_type = language_v1.EncodingType.UTF8

        response = self.client.analyze_entity_sentiment(request={'document': document, 'encoding_type': encoding_type})

        return response.entities


        # Get the language of the text, which will be the same as
        # the language specified in the request or, if not specified,
        # the automatically-detected language.
        # print(u"Language of the text: {}".format(response.language))
        # return entity


