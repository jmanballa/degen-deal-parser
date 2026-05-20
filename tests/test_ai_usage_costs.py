import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app.models import ParseAttempt
from app.discord import parser as parser_module
from app.discord.parser import extract_usage_metrics, parse_deal_with_ai, parse_deal_with_ai_async


class AiUsageCostTests(unittest.TestCase):
    def test_gpt55_nvidia_chat_usage_records_nonzero_cost(self):
        response = SimpleNamespace(
            usage=SimpleNamespace(
                prompt_tokens=1200,
                completion_tokens=340,
                total_tokens=1540,
                prompt_tokens_details=SimpleNamespace(cached_tokens=200),
            )
        )

        metrics = extract_usage_metrics(response, model="openai/openai/gpt-5.5")

        self.assertEqual(metrics["input_tokens"], 1200)
        self.assertEqual(metrics["cached_input_tokens"], 200)
        self.assertEqual(metrics["output_tokens"], 340)
        self.assertEqual(metrics["total_tokens"], 1540)
        self.assertEqual(metrics["estimated_cost_usd"], 0.004675)
        self.assertGreater(metrics["estimated_cost_usd"], 0)

    def test_nested_openai_compatible_usage_shape_records_cost(self):
        response = {
            "response": {
                "usage": {
                    "prompt_tokens": "900",
                    "completion_tokens": "100",
                    "total_tokens": "1000",
                }
            }
        }

        metrics = extract_usage_metrics(response, model="gpt-5.5")

        self.assertEqual(metrics["input_tokens"], 900)
        self.assertEqual(metrics["output_tokens"], 100)
        self.assertEqual(metrics["total_tokens"], 1000)
        self.assertGreater(metrics["estimated_cost_usd"], 0)

    def test_responses_api_usage_shape_still_works(self):
        response = SimpleNamespace(
            usage=SimpleNamespace(
                input_tokens=2000,
                output_tokens=100,
                total_tokens=2100,
                input_tokens_details=SimpleNamespace(cached_tokens=250),
            )
        )

        metrics = extract_usage_metrics(response, model="gpt-5-nano")

        self.assertEqual(metrics["input_tokens"], 2000)
        self.assertEqual(metrics["cached_input_tokens"], 250)
        self.assertEqual(metrics["output_tokens"], 100)
        self.assertEqual(metrics["total_tokens"], 2100)
        self.assertEqual(metrics["estimated_cost_usd"], 0.000129)

    def test_parse_attempt_can_record_provider_and_model(self):
        attempt = ParseAttempt(
            message_id=1,
            attempt_number=1,
            model_used="openai/openai/gpt-5.5",
            provider_used="nvidia",
        )

        self.assertEqual(attempt.model_used, "openai/openai/gpt-5.5")
        self.assertEqual(attempt.provider_used, "nvidia")

    def test_ai_parse_metadata_includes_provider_model_and_usage(self):
        class FakeCompletions:
            def create(self, **_kwargs):
                return SimpleNamespace(
                    choices=[
                        SimpleNamespace(
                            message=SimpleNamespace(
                                content=json.dumps(
                                    {
                                        "parsed_type": "sell",
                                        "parsed_amount": 10.0,
                                        "parsed_payment_method": "cash",
                                        "parsed_cash_direction": "none",
                                        "parsed_category": "singles",
                                        "parsed_items": [],
                                        "parsed_items_in": [],
                                        "parsed_items_out": [],
                                        "parsed_trade_summary": "",
                                        "parsed_notes": "test parse",
                                        "image_summary": "no image used",
                                        "confidence": 0.95,
                                        "needs_review": False,
                                    }
                                )
                            )
                        )
                    ],
                    usage=SimpleNamespace(
                        prompt_tokens=120,
                        completion_tokens=30,
                        total_tokens=150,
                    ),
                )

        class FakeClient:
            chat = SimpleNamespace(completions=FakeCompletions())

            def with_options(self, **_kwargs):
                return self

        with patch("app.discord.parser.get_ai_client", return_value=FakeClient()), patch(
            "app.discord.parser.get_relevant_correction_hints", return_value=[]
        ):
            parsed = parse_deal_with_ai(
                author_name="cashier",
                message_text="sold card $10 cash",
                image_urls=[],
                channel_name="store-sales",
            )

        self.assertEqual(parsed["_ai_provider"], parser_module.get_provider())
        self.assertEqual(parsed["_openai_model"], parser_module.MODEL)
        self.assertEqual(parsed["_openai_usage"]["input_tokens"], 120)
        self.assertEqual(parsed["_openai_usage"]["output_tokens"], 30)
        self.assertGreater(parsed["_openai_usage"]["estimated_cost_usd"], 0)

    def test_async_parser_passes_expected_call_arguments(self):
        captured = {}

        async def fake_to_thread(func, *args):
            captured["func"] = func
            captured["args"] = args
            return {"ok": True}

        with patch("app.discord.parser.asyncio.to_thread", side_effect=fake_to_thread):
            result = asyncio.run(
                parse_deal_with_ai_async(
                    author_name="cashier",
                    message_text="$10 cash",
                    image_urls=[],
                    channel_name="store-sales",
                )
            )

        self.assertEqual(result, {"ok": True})
        self.assertEqual(captured["args"], ("cashier", "$10 cash", [], "store-sales"))


if __name__ == "__main__":
    unittest.main()
