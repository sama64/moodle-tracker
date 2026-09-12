from types import SimpleNamespace

import pytest

from uni_tracker.services.llm import (
    LLMUnavailable,
    _build_request_payload,
    resolve_llm_config,
)


def test_resolves_generic_deepseek_configuration() -> None:
    settings = SimpleNamespace(
        llm_provider="DeepSeek",
        llm_api_key="secret",
        llm_api_url="https://api.deepseek.com",
        llm_model="deepseek-flash",
        nvidia_api_key="legacy-secret",
        nvidia_api_url="https://legacy.invalid/v1/chat/completions",
        nvidia_model="legacy-model",
    )

    config = resolve_llm_config(settings)

    assert config is not None
    assert config.provider == "deepseek"
    assert config.api_url == "https://api.deepseek.com/chat/completions"
    assert config.model == "deepseek-flash"
    assert config.extractor_type == "llm_deepseek_flash"


def test_deepseek_payload_requests_json_without_nvidia_options() -> None:
    config = resolve_llm_config(
        SimpleNamespace(
            llm_provider="deepseek",
            llm_api_key="secret",
            llm_api_url="https://api.deepseek.com",
            llm_model="deepseek-flash",
            nvidia_api_key=None,
        )
    )
    assert config is not None

    payload = _build_request_payload(config, "Return JSON")

    assert payload["response_format"] == {"type": "json_object"}
    assert "chat_template_kwargs" not in payload


def test_legacy_nvidia_configuration_still_works() -> None:
    settings = SimpleNamespace(
        nvidia_api_key="legacy-secret",
        nvidia_api_url="https://integrate.api.nvidia.com/v1/chat/completions",
        nvidia_model="moonshotai/kimi-k2.5",
    )

    config = resolve_llm_config(settings)

    assert config is not None
    assert config.provider == "nvidia"
    assert config.api_url.endswith("/v1/chat/completions")
    assert _build_request_payload(config, "Return JSON")["chat_template_kwargs"] == {"thinking": True}


def test_generic_configuration_requires_url_and_model() -> None:
    with pytest.raises(LLMUnavailable):
        resolve_llm_config(
            SimpleNamespace(
                llm_provider="deepseek",
                llm_api_key="secret",
                llm_api_url="",
                llm_model="",
            )
        )
