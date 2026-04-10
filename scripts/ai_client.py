"""
Unified AI Client
Provides a unified interface for multiple AI providers:
  - deepseek: DeepSeek Chat  (env: DEEPSEEK_API_TOKEN)
  - gemini:   Google Gemini   (env: GEMINI_API_TOKEN)
  - openai:   OpenAI GPT      (env: OPENAI_API_TOKEN)
  - azure:    Azure OpenAI    (env: AZURE_OPENAI_KEY + OPENAI_BASE_URL)
"""

import os
import time
import threading

# ---------------------------------------------------------------------------
# Gemini SDK – try the newer google-genai first, fall back to legacy
# ---------------------------------------------------------------------------
_GEMINI_NEW_SDK = False
GEMINI_AVAILABLE = False

try:
    from google import genai                    # google-genai (new)
    GEMINI_AVAILABLE = True
    _GEMINI_NEW_SDK = True
except ImportError:
    try:
        import google.generativeai as genai     # google-generativeai (legacy)
        GEMINI_AVAILABLE = True
    except ImportError:
        genai = None

# ---------------------------------------------------------------------------
# Thread-safe printing (also re-exported for callers)
# ---------------------------------------------------------------------------
print_lock = threading.Lock()

def thread_safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

# ---------------------------------------------------------------------------
# API keys / endpoints  (read once at import time)
# ---------------------------------------------------------------------------
DEEPSEEK_API_KEY      = os.getenv("DEEPSEEK_API_TOKEN")
DEEPSEEK_BASE_URL     = "https://api.deepseek.com"
GEMINI_API_KEY        = os.getenv("GEMINI_API_TOKEN")
OPENAI_API_KEY        = os.getenv("OPENAI_API_TOKEN")
AZURE_OPENAI_KEY      = os.getenv("AZURE_OPENAI_KEY")
AZURE_OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL")

# ---------------------------------------------------------------------------
# Model names  (override via env var if needed)
# ---------------------------------------------------------------------------
DEEPSEEK_MODEL_NAME       = os.getenv("DEEPSEEK_MODEL_NAME", "deepseek-chat")
GEMINI_MODEL_NAME         = os.getenv("GEMINI_MODEL_NAME", "gemini-3-flash-preview")
OPENAI_MODEL_NAME         = os.getenv("OPENAI_MODEL_NAME", "gpt-4.1")
AZURE_OPENAI_MODEL_NAME   = os.getenv("AZURE_OPENAI_MODEL_NAME", "gpt-5.4")

# ---------------------------------------------------------------------------
# Provider-specific output-token limits
# ---------------------------------------------------------------------------
AI_MAX_TOKENS_DEEPSEEK = 8192
AI_MAX_TOKENS_GEMINI   = 8192
AI_MAX_TOKENS_OPENAI   = 16384
AI_MAX_TOKENS_AZURE    = 16384

PROVIDER_MAX_TOKENS = {
    "deepseek": AI_MAX_TOKENS_DEEPSEEK,
    "gemini":   AI_MAX_TOKENS_GEMINI,
    "openai":   AI_MAX_TOKENS_OPENAI,
    "azure":    AI_MAX_TOKENS_AZURE,
}

# ---------------------------------------------------------------------------
# Gemini rate-limiting state
# ---------------------------------------------------------------------------
_gemini_call_count = 0
GEMINI_RATE_LIMIT_CALLS = 10
GEMINI_RATE_LIMIT_SLEEP = 30


# ---------------------------------------------------------------------------
# Unified AI Client
# ---------------------------------------------------------------------------
class UnifiedAIClient:
    """Unified interface for different AI providers."""

    def __init__(self, provider="deepseek"):
        self.provider = provider

        if provider == "deepseek":
            from openai import OpenAI
            self.client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL)
            self.model = DEEPSEEK_MODEL_NAME
            self.max_tokens = AI_MAX_TOKENS_DEEPSEEK

        elif provider == "gemini":
            if not GEMINI_AVAILABLE:
                raise ImportError(
                    "Neither google-genai nor google-generativeai is installed. "
                    "Run: pip install google-genai  (or)  pip install google-generativeai"
                )
            if not GEMINI_API_KEY:
                raise ValueError("GEMINI_API_TOKEN environment variable must be set")
            if _GEMINI_NEW_SDK:
                self.client = genai.Client(api_key=GEMINI_API_KEY)
            else:
                genai.configure(api_key=GEMINI_API_KEY)
                self.client = None
            self.model = GEMINI_MODEL_NAME
            self.max_tokens = AI_MAX_TOKENS_GEMINI

        elif provider == "openai":
            from openai import OpenAI
            if not OPENAI_API_KEY:
                raise ValueError("OPENAI_API_TOKEN environment variable must be set")
            self.client = OpenAI(api_key=OPENAI_API_KEY)
            self.model = OPENAI_MODEL_NAME
            self.max_tokens = AI_MAX_TOKENS_OPENAI

        elif provider == "azure":
            from openai import OpenAI
            if not AZURE_OPENAI_KEY:
                raise ValueError("AZURE_OPENAI_KEY environment variable must be set")
            if not AZURE_OPENAI_BASE_URL:
                raise ValueError("OPENAI_BASE_URL environment variable must be set")
            self.client = OpenAI(api_key=AZURE_OPENAI_KEY, base_url=AZURE_OPENAI_BASE_URL)
            self.model = AZURE_OPENAI_MODEL_NAME
            self.max_tokens = AI_MAX_TOKENS_AZURE

        else:
            raise ValueError(f"Unsupported AI provider: {provider}")

    # -----------------------------------------------------------------------
    def chat_completion(self, messages, temperature=0.1, max_tokens=None):
        """Unified chat completion interface."""
        if max_tokens is None:
            max_tokens = self.max_tokens
        max_tokens = min(max_tokens, self.max_tokens)

        if self.provider in ("deepseek", "openai"):
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            return response.choices[0].message.content.strip()

        elif self.provider == "azure":
            return self._azure_chat(messages, temperature, max_tokens)

        elif self.provider == "gemini":
            return self._gemini_chat(messages)

    # -----------------------------------------------------------------------
    def _azure_chat(self, messages, temperature, max_tokens):
        """Azure OpenAI via the Responses API (client.responses.create)."""
        instructions = None
        input_parts = []
        for msg in messages:
            if msg.get("role") == "system":
                instructions = msg.get("content", "")
            else:
                input_parts.append(msg)

        input_value = input_parts if input_parts else messages

        kwargs = dict(
            model=self.model,
            input=input_value,
            temperature=temperature,
            max_output_tokens=max_tokens,
        )
        if instructions:
            kwargs["instructions"] = instructions

        response = self.client.responses.create(**kwargs)
        return response.output_text.strip()

    # -----------------------------------------------------------------------
    def _gemini_chat(self, messages):
        global _gemini_call_count
        try:
            _gemini_call_count += 1
            if _gemini_call_count > GEMINI_RATE_LIMIT_CALLS:
                thread_safe_print(
                    f"   ⏳ Rate limit: {GEMINI_RATE_LIMIT_CALLS} calls reached, "
                    f"sleeping {GEMINI_RATE_LIMIT_SLEEP}s..."
                )
                time.sleep(GEMINI_RATE_LIMIT_SLEEP)
                _gemini_call_count = 1

            prompt = self._convert_messages_to_prompt(messages)
            thread_safe_print(
                f"   🔄 Calling Gemini API ({_gemini_call_count}/{GEMINI_RATE_LIMIT_CALLS})..."
            )

            if _GEMINI_NEW_SDK:
                response = self.client.models.generate_content(
                    model=self.model, contents=prompt
                )
            else:
                model = genai.GenerativeModel(self.model)
                response = model.generate_content(prompt)

            if response and response.text:
                thread_safe_print("   ✅ Gemini response received")
                return response.text.strip()

            thread_safe_print("   ⚠️  Gemini response was empty or blocked")
            return "No response from Gemini"

        except Exception as e:
            thread_safe_print(f"   ❌ Gemini API error: {e}")
            thread_safe_print(
                "   💡 Consider switching to DeepSeek: AI_PROVIDER = 'deepseek'"
            )
            raise

    # -----------------------------------------------------------------------
    @staticmethod
    def _convert_messages_to_prompt(messages):
        """Convert OpenAI-style messages to a single prompt for Gemini."""
        parts = []
        for msg in messages:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            if role == "system":
                parts.append(f"System: {content}")
            else:
                parts.append(content)
        return "\n\n".join(parts)
