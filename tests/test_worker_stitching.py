import unittest
from datetime import timedelta

from app.models import DiscordMessage, utcnow
from app.discord.worker import is_short_fragment, should_stitch_rows


class WorkerStitchingTests(unittest.TestCase):
    def make_row(self, content: str, *, seconds: int = 0, has_image: bool = False) -> DiscordMessage:
        return DiscordMessage(
            discord_message_id=f"msg-{content}-{seconds}",
            channel_id="chan-1",
            channel_name="store-sales-and-trades",
            author_id="author-1",
            author_name="tester",
            content=content,
            attachment_urls_json='["https://example.test/card.jpg"]' if has_image else "[]",
            created_at=utcnow() + timedelta(seconds=seconds),
        )

    def test_short_explicit_image_deal_is_not_fragment(self) -> None:
        row = self.make_row("Buy for $374", has_image=True)

        self.assertFalse(is_short_fragment(row))

    def test_back_to_back_image_deals_do_not_steal_trade_fragment(self) -> None:
        buy_row = self.make_row("Buy for $374", seconds=0, has_image=True)
        trade_row = self.make_row("Top Put and Bottom In", seconds=15, has_image=True)

        self.assertFalse(should_stitch_rows(buy_row, [buy_row, trade_row]))

    def test_image_then_explicit_text_still_force_stitches(self) -> None:
        image_row = self.make_row("", seconds=0, has_image=True)
        text_row = self.make_row("Buy 450 cash", seconds=25, has_image=False)

        self.assertTrue(should_stitch_rows(image_row, [image_row, text_row]))


if __name__ == "__main__":
    unittest.main()
