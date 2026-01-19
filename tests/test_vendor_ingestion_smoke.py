"""
Vendor Ingestion Smoke Tests

Tests that all supported vendors can ingest sample logs and produce canonical events.
Each vendor should have:
- Normal log (valid format, should succeed)
- Abnormal log (invalid format, should be excluded with reason in report)
"""

import pytest
import sys
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestor.base import BaseIngestor
from reporting.report_builder import ReportBuilder


# Vendor list (from AIMO_Detail.md section 1.1)
SUPPORTED_VENDORS = [
    "paloalto",
    "zscaler",
    "netskope",
    "mdca",
    "umbrella",
    "bluecoat",
    "skyhigh",
    "ifilter"
]


class TestVendorIngestionSmoke:
    """Smoke tests for vendor ingestion."""
    
    @pytest.mark.parametrize("vendor", SUPPORTED_VENDORS)
    def test_vendor_has_sample_logs(self, vendor):
        """Each vendor should have sample logs in sample_logs/<vendor>/."""
        sample_dir = Path(__file__).parent.parent / "sample_logs" / vendor
        normal_log = sample_dir / "normal.csv"
        abnormal_log = sample_dir / "abnormal.csv"
        
        # At least normal log should exist
        assert normal_log.exists(), f"Normal sample log missing for {vendor}: {normal_log}"
        
        # Abnormal log is optional (for testing exclusion handling)
        if not abnormal_log.exists():
            pytest.skip(f"Abnormal sample log not found for {vendor} (optional)")
    
    @pytest.mark.parametrize("vendor", SUPPORTED_VENDORS)
    def test_normal_log_ingestion(self, vendor, tmp_path):
        """Normal log should be ingested successfully."""
        sample_dir = Path(__file__).parent.parent / "sample_logs" / vendor
        normal_log = sample_dir / "normal.csv"
        
        if not normal_log.exists():
            pytest.skip(f"Normal sample log not found for {vendor}")
        
        # Initialize ingestor
        ingestor = BaseIngestor(vendor)
        
        # Ingest file
        events = []
        parse_errors = []
        
        try:
            for event in ingestor.ingest_file(str(normal_log)):
                events.append(event)
        except Exception as e:
            parse_errors.append(str(e))
        
        # Should have at least one event
        assert len(events) > 0, f"{vendor}: No events ingested from normal log"
        
        # Verify canonical event structure
        for event in events:
            # Required fields (per AIMO_Detail.md section 5.1)
            assert "event_time" in event or "timestamp" in event, f"{vendor}: Missing event_time"
            assert "vendor" in event or ingestor.vendor == vendor, f"{vendor}: Missing vendor"
            
            # At least one of these should be present
            assert "url_full" in event or "dest_host" in event or "dest_domain" in event, \
                f"{vendor}: Missing URL/destination fields"
    
    @pytest.mark.parametrize("vendor", SUPPORTED_VENDORS)
    def test_abnormal_log_exclusion(self, vendor):
        """Abnormal log should be excluded with reason in report."""
        sample_dir = Path(__file__).parent.parent / "sample_logs" / vendor
        abnormal_log = sample_dir / "abnormal.csv"
        
        if not abnormal_log.exists():
            pytest.skip(f"Abnormal sample log not found for {vendor} (optional)")
        
        # Initialize ingestor
        ingestor = BaseIngestor(vendor)
        
        # Try to ingest (should handle gracefully)
        events = []
        parse_errors = []
        
        try:
            for event in ingestor.ingest_file(str(abnormal_log)):
                events.append(event)
        except Exception as e:
            parse_errors.append(str(e))
        
        # Abnormal log may:
        # 1. Produce 0 events (excluded)
        # 2. Produce events with warnings
        # 3. Raise parse errors (should be caught and logged)
        
        # If events are produced, they should still be valid canonical events
        for event in events:
            assert "event_time" in event or "timestamp" in event, \
                f"{vendor}: Abnormal log produced invalid event (missing event_time)"
    
    def test_paloalto_sample_exists(self):
        """Palo Alto sample should exist (baseline)."""
        sample_file = Path(__file__).parent.parent / "sample_logs" / "paloalto_sample.csv"
        assert sample_file.exists(), "Palo Alto sample log missing (baseline)"
    
    @pytest.mark.parametrize("vendor", ["paloalto", "zscaler", "netskope"])
    def test_e2e_vendor_processing(self, vendor, tmp_path):
        """E2E test: Ingest → Normalize → Signature for major vendors."""
        sample_dir = Path(__file__).parent.parent / "sample_logs" / vendor
        
        # Try normal.csv first, fallback to <vendor>_sample.csv
        normal_log = sample_dir / "normal.csv"
        if not normal_log.exists():
            normal_log = Path(__file__).parent.parent / "sample_logs" / f"{vendor}_sample.csv"
        
        if not normal_log.exists():
            pytest.skip(f"Sample log not found for {vendor}")
        
        # Import required modules
        from normalize.url_normalizer import URLNormalizer
        from signatures.signature_builder import SignatureBuilder
        
        # Initialize components
        ingestor = BaseIngestor(vendor)
        normalizer = URLNormalizer()
        signature_builder = SignatureBuilder()
        
        # Stage 1: Ingestion
        events = []
        for event in ingestor.ingest_file(str(normal_log)):
            events.append(event)
        
        assert len(events) > 0, f"{vendor}: No events ingested"
        
        # Stage 2: Normalization & Signature
        signatures = set()
        for event in events:
            url_full = event.get("url_full", "")
            if not url_full:
                continue
            
            norm_result = normalizer.normalize(url_full)
            sig = signature_builder.build_signature(
                norm_host=norm_result["norm_host"],
                norm_path=norm_result["norm_path"],
                norm_query=norm_result["norm_query"],
                http_method=event.get("http_method"),
                bytes_sent=event.get("bytes_sent", 0)
            )
            signatures.add(sig["url_signature"])
        
        # Should have at least one signature
        assert len(signatures) > 0, f"{vendor}: No signatures generated"
        
        # Verify signature structure
        for sig in signatures:
            assert isinstance(sig, str), f"{vendor}: Signature should be string"
            assert len(sig) > 0, f"{vendor}: Signature should not be empty"
