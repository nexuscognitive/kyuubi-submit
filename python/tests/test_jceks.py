"""Tests for JCEKS password extraction in kyuubi-submit."""

import importlib
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Add the parent directory so we can import kyuubi-submit module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

kyuubi_submit = importlib.import_module("kyuubi-submit")
extract_password_from_jceks = kyuubi_submit.extract_password_from_jceks
get_password = kyuubi_submit.get_password


class TestExtractPasswordFromJceks(unittest.TestCase):

    @patch.dict('sys.modules', {'jks': MagicMock()})
    def test_extract_returns_stored_password(self):
        import jks
        mock_entry = MagicMock()
        mock_entry.key = b's3cret!'
        mock_ks = MagicMock()
        mock_ks.secret_keys = {'myuser': mock_entry}
        jks.KeyStore.load.return_value = mock_ks

        # Re-import to pick up mock
        result = extract_password_from_jceks('/fake/path.jceks', 'myuser')
        self.assertEqual(result, 's3cret!')
        jks.KeyStore.load.assert_called_once_with('/fake/path.jceks', 'none')

    @patch.dict('sys.modules', {'jks': MagicMock()})
    def test_extract_raises_on_missing_alias(self):
        import jks
        mock_ks = MagicMock()
        # Use a real dict so .get('wrong-alias') returns None
        mock_ks.secret_keys = {'otheruser': MagicMock()}
        jks.KeyStore.load.return_value = mock_ks

        with self.assertRaises(ValueError) as ctx:
            extract_password_from_jceks('/fake/path.jceks', 'wrong-alias')
        self.assertIn('wrong-alias', str(ctx.exception))
        self.assertIn('not found', str(ctx.exception))


class TestGetPasswordJceksIntegration(unittest.TestCase):

    @patch('os.path.isfile', return_value=True)
    def test_get_password_resolves_jceks_path(self, mock_isfile):
        with patch.dict('sys.modules', {'jks': MagicMock()}):
            import jks
            mock_entry = MagicMock()
            mock_entry.key = b'extracted-password'
            mock_ks = MagicMock()
            mock_ks.secret_keys = {'myuser': mock_entry}
            jks.KeyStore.load.return_value = mock_ks

            config = {'password': '/path/to/creds.jceks'}
            result = get_password(config, 'myuser')
            self.assertEqual(result, 'extracted-password')

    def test_get_password_returns_plain_password_as_is(self):
        config = {'password': 'plain-password'}
        result = get_password(config, 'someuser')
        self.assertEqual(result, 'plain-password')

    @patch.dict(os.environ, {'KYUUBI_SUBMIT_PASSWORD': 'env-password'})
    def test_get_password_from_env_var(self):
        config = {}
        result = get_password(config, 'someuser')
        self.assertEqual(result, 'env-password')


class TestTokenAuth(unittest.TestCase):

    def test_token_sets_bearer_header(self):
        submitter = kyuubi_submit.KyuubiBatchSubmitter(
            server='https://kyuubi.example.com',
            username='user',
            password=None,
            logger=MagicMock(),
            token='my-token-123',
        )
        self.assertEqual(
            submitter.session.headers.get('Authorization'),
            'Bearer my-token-123',
        )
        self.assertIsNone(submitter.session.auth)

    def test_password_takes_priority_over_token(self):
        submitter = kyuubi_submit.KyuubiBatchSubmitter(
            server='https://kyuubi.example.com',
            username='user',
            password='mypassword',
            logger=MagicMock(),
            token='my-token-123',
        )
        self.assertEqual(submitter.session.auth, ('user', 'mypassword'))
        self.assertNotIn('Authorization', submitter.session.headers)

    def test_password_without_token_uses_basic_auth(self):
        submitter = kyuubi_submit.KyuubiBatchSubmitter(
            server='https://kyuubi.example.com',
            username='user',
            password='mypassword',
            logger=MagicMock(),
        )
        self.assertEqual(submitter.session.auth, ('user', 'mypassword'))


if __name__ == '__main__':
    unittest.main()
