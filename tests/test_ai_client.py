import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from ai_client import (
    AI_MAX_TOKENS_AZURE,
    AI_MAX_TOKENS_DEEPSEEK,
    AI_MAX_TOKENS_OPENAI,
    UnifiedAIClient,
    get_provider_max_tokens,
)


class _RecordingResponsesClient:
    def __init__(self, output_text="translated", status="completed"):
        self.kwargs = None
        self.output_text = output_text
        self.status = status

    def create(self, **kwargs):
        self.kwargs = kwargs
        incomplete_details = (
            SimpleNamespace(reason="max_output_tokens")
            if self.status == "incomplete"
            else None
        )
        return SimpleNamespace(
            output_text=self.output_text,
            status=self.status,
            incomplete_details=incomplete_details,
        )


class _RecordingChatCompletionsClient:
    def __init__(self):
        self.kwargs = None

    def create(self, **kwargs):
        self.kwargs = kwargs
        message = SimpleNamespace(content="translated")
        return SimpleNamespace(
            choices=[SimpleNamespace(message=message, finish_reason="stop")]
        )


class AIClientTokenLimitTest(unittest.TestCase):
    def test_azure_gpt_uses_65536_token_limit(self):
        responses = _RecordingResponsesClient()
        ai_client = object.__new__(UnifiedAIClient)
        ai_client.provider = "azure"
        ai_client.model = "gpt-5.4"
        ai_client.max_tokens = AI_MAX_TOKENS_AZURE
        ai_client.client = SimpleNamespace(responses=responses)

        result = ai_client.chat_completion(
            [{"role": "user", "content": "Translate this document."}],
            max_tokens=100_000,
        )

        self.assertEqual(result, "translated")
        self.assertEqual(AI_MAX_TOKENS_AZURE, 65_536)
        self.assertEqual(responses.kwargs["max_output_tokens"], 65_536)
        self.assertNotEqual(
            responses.kwargs["max_output_tokens"],
            AI_MAX_TOKENS_DEEPSEEK,
        )

    def test_provider_limits_do_not_fall_back_to_deepseek(self):
        self.assertEqual(get_provider_max_tokens("azure"), 65_536)
        with self.assertRaisesRegex(ValueError, "Unsupported AI provider"):
            get_provider_max_tokens("gpt")

    def test_openai_gpt_41_uses_32768_token_limit(self):
        completions = _RecordingChatCompletionsClient()
        ai_client = object.__new__(UnifiedAIClient)
        ai_client.provider = "openai"
        ai_client.model = "gpt-4.1"
        ai_client.max_tokens = AI_MAX_TOKENS_OPENAI
        ai_client.client = SimpleNamespace(
            chat=SimpleNamespace(completions=completions)
        )

        result = ai_client.chat_completion(
            [{"role": "user", "content": "Translate this document."}],
            max_tokens=100_000,
        )

        self.assertEqual(result, "translated")
        self.assertEqual(AI_MAX_TOKENS_OPENAI, 32_768)
        self.assertEqual(completions.kwargs["max_tokens"], 32_768)

    def test_deepseek_chat_stays_at_8192_token_api_limit(self):
        self.assertEqual(AI_MAX_TOKENS_DEEPSEEK, 8_192)
        self.assertEqual(get_provider_max_tokens("deepseek"), 8_192)

    def test_azure_missing_output_text_raises_clear_error(self):
        responses = _RecordingResponsesClient(output_text=None, status="incomplete")
        ai_client = object.__new__(UnifiedAIClient)
        ai_client.provider = "azure"
        ai_client.model = "gpt-5.4"
        ai_client.max_tokens = AI_MAX_TOKENS_AZURE
        ai_client.client = SimpleNamespace(responses=responses)

        with self.assertRaisesRegex(RuntimeError, "did not include output text"):
            ai_client.chat_completion(
                [{"role": "user", "content": "Translate this document."}]
            )


if __name__ == "__main__":
    unittest.main()
