from unittest import TestCase, main
from unittest.mock import Mock

from app.tweeter import tweet


class TestTweet(TestCase):

    def test_tweet(self):
        mock_api = Mock()
        msg = 'message'
        tweet(mock_api, msg)
        result = mock_api.mock_calls
        print(result)
        mock_api.PostUpdate.assert_called_with(msg)


if __name__ == '__main__':
    main()
