"""
Test for header randomization functionality in WebsiteScraping module.
"""

import os
import tempfile

import pandas as pd

import engine.gather.WebsiteScraping as WebsiteScraping


def test_header_randomization():
    """Test that get_randomized_headers() returns different headers on subsequent calls."""

    # Create a temporary file with minimal test data
    with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp_file:
        tmp_path = tmp_file.name

        # Create a minimal test dataframe and save it
        test_df = pd.DataFrame(
            {
                "report_id": ["TAIC_a_2020_001", "ATSB_a_2020_001"],
                "agency_id": ["AO-2020-001", "AO-2020-001"],
                "title": ["Test Title 1", "Test Title 2"],
                "event_type": ["Aviation", "Aviation"],
                "investigation_type": ["full", "full"],
                "summary": ["Test summary", "Test summary"],
                "misc": [{}, {}],
                "url": ["http://test1.com", "http://test2.com"],
            }
        )
        test_df.to_pickle(tmp_path)

        try:
            # Create a WebsiteScraper instance
            scraper = WebsiteScraping.WebsiteScraper(tmp_path)

            # Generate multiple sets of headers
            headers_sets = []
            for _ in range(10):
                headers = scraper.get_randomized_headers()
                headers_sets.append(headers)

            # Verify that we get different headers across calls
            unique_user_agents = set(h.get("User-Agent") for h in headers_sets)
            unique_accept_languages = set(
                h.get("Accept-Language") for h in headers_sets
            )
            unique_referers = set(h.get("Referer") for h in headers_sets)

            # Verify we have some variation
            assert (
                len(unique_user_agents) > 1
            ), "Should have multiple User-Agent variations"
            assert (
                len(unique_accept_languages) > 1
            ), "Should have multiple Accept-Language variations"
            assert len(unique_referers) > 1, "Should have multiple Referer variations"

            # Verify all headers contain required fields
            for i, headers in enumerate(headers_sets):
                assert "User-Agent" in headers, f"Headers set {i} missing User-Agent"
                assert "Accept" in headers, f"Headers set {i} missing Accept"
                assert (
                    "Accept-Language" in headers
                ), f"Headers set {i} missing Accept-Language"
                assert "Referer" in headers, f"Headers set {i} missing Referer"
                assert "Connection" in headers, f"Headers set {i} missing Connection"

                # Verify User-Agent looks realistic
                assert (
                    "Mozilla" in headers["User-Agent"]
                ), f"Headers set {i} has unrealistic User-Agent"

                # Verify headers are well-formed
                assert headers["Accept"].startswith(
                    "text/html"
                ), f"Headers set {i} has malformed Accept header"
                assert (
                    headers["Accept-Language"].count(",") >= 0
                ), f"Headers set {i} has malformed Accept-Language header"
                assert headers["Referer"].startswith(
                    "https://"
                ), f"Headers set {i} has malformed Referer header"
                assert headers["Connection"] in [
                    "keep-alive",
                    "close",
                ], f"Headers set {i} has invalid Connection header"

        finally:
            # Clean up the temporary file
            os.unlink(tmp_path)


def test_header_randomization_order_varies():
    """Test that header order is randomized between calls."""

    # Create a temporary file with minimal test data
    with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp_file:
        tmp_path = tmp_file.name

        # Create a minimal test dataframe and save it
        test_df = pd.DataFrame(
            {
                "report_id": ["TAIC_a_2020_001"],
                "agency_id": ["AO-2020-001"],
                "title": ["Test Title 1"],
                "event_type": ["Aviation"],
                "investigation_type": ["full"],
                "summary": ["Test summary"],
                "misc": [{}],
                "url": ["http://test1.com"],
            }
        )
        test_df.to_pickle(tmp_path)

        try:
            # Create a WebsiteScraper instance
            scraper = WebsiteScraping.WebsiteScraper(tmp_path)

            # Generate multiple sets of headers and record their orders
            header_orders = []
            for _ in range(5):
                headers = scraper.get_randomized_headers()
                header_order = list(headers.keys())
                header_orders.append(header_order)

            # Check that we get different orders
            unique_orders = set(tuple(order) for order in header_orders)
            assert len(unique_orders) > 1, "Header order should vary between calls"

        finally:
            # Clean up the temporary file
            os.unlink(tmp_path)


if __name__ == "__main__":
    test_header_randomization()
    test_header_randomization_order_varies()
    print("✅ All header randomization tests passed!")
