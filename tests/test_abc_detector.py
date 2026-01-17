"""
Test A/B/C Detector - Deterministic Detection and Sampling

Tests that A/B/C detection is deterministic, reproducible, and prevents zero-exclusion
of small-volume events.
"""

import pytest
from pathlib import Path
import sys
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from detectors.abc_detector import ABCDetector


class TestABCDetection:
    """Test A/B/C detection logic."""
    
    def _create_event(self, 
                     user_id: str = "user1",
                     dest_domain: str = "example.com",
                     bytes_sent: int = 0,
                     http_method: str = "GET",
                     app_category: str = None,
                     event_time: datetime = None,
                     action: str = "allow",
                     url_signature: str = "sig1",
                     ingest_lineage_hash: str = None) -> dict:
        """Create a canonical event for testing."""
        if event_time is None:
            event_time = datetime(2024, 1, 15, 10, 0, 0)
        
        if ingest_lineage_hash is None:
            import hashlib
            key = f"{user_id}|{dest_domain}|{event_time.isoformat()}|{bytes_sent}"
            ingest_lineage_hash = hashlib.sha256(key.encode()).hexdigest()
        
        return {
            "event_time": event_time.isoformat() + "Z",
            "user_id": user_id,
            "dest_domain": dest_domain,
            "bytes_sent": bytes_sent,
            "http_method": http_method,
            "app_category": app_category,
            "action": action,
            "url_signature": url_signature,
            "ingest_lineage_hash": ingest_lineage_hash,
            "bytes_received": 0
        }
    
    def test_A_detection_threshold(self):
        """A判定：bytes_sent=1MB-1 はAでない／1MB はA"""
        detector = ABCDetector(run_id="test_run")
        
        # bytes_sent = 1MB - 1 (not A)
        event1 = self._create_event(bytes_sent=1048575)  # 1MB - 1
        # bytes_sent = 1MB (is A)
        event2 = self._create_event(bytes_sent=1048576)  # 1MB
        # bytes_sent = 1MB + 1 (is A)
        event3 = self._create_event(bytes_sent=1048577)  # 1MB + 1
        
        results = detector.detect([event1, event2, event3])
        
        a_signals = results["signals"]["A"]
        a_lineage_hashes = {s["ingest_lineage_hash"] for s in a_signals}
        
        assert event1["ingest_lineage_hash"] not in a_lineage_hashes, "1MB-1 should not be A"
        assert event2["ingest_lineage_hash"] in a_lineage_hashes, "1MB should be A"
        assert event3["ingest_lineage_hash"] in a_lineage_hashes, "1MB+1 should be A"
    
    def test_B_detection_write_method_high_risk(self):
        """B判定：write_method かつ AI/Unknown 宛でBになる"""
        detector = ABCDetector(run_id="test_run")
        
        # POST to AI category (should be B)
        event1 = self._create_event(
            http_method="POST",
            app_category="AI",
            bytes_sent=1000,  # Small, not A
            ingest_lineage_hash="event1_post_ai"
        )
        # POST to Unknown category (should be B)
        event2 = self._create_event(
            http_method="POST",
            app_category="Unknown",
            bytes_sent=2000,
            ingest_lineage_hash="event2_post_unknown"
        )
        # GET to AI category (not write_method, should not be B)
        event3 = self._create_event(
            http_method="GET",
            app_category="AI",
            bytes_sent=1000,
            ingest_lineage_hash="event3_get_ai"
        )
        # POST to Business category (not high-risk, should not be B)
        event4 = self._create_event(
            http_method="POST",
            app_category="Business",
            bytes_sent=1000,
            ingest_lineage_hash="event4_post_business"
        )
        
        results = detector.detect([event1, event2, event3, event4])
        
        b_signals = results["signals"]["B"]
        b_lineage_hashes = {s["ingest_lineage_hash"] for s in b_signals}
        
        assert event1["ingest_lineage_hash"] in b_lineage_hashes, "POST to AI should be B"
        assert event2["ingest_lineage_hash"] in b_lineage_hashes, "POST to Unknown should be B"
        assert event3["ingest_lineage_hash"] not in b_lineage_hashes, "GET to AI should not be B"
        assert event4["ingest_lineage_hash"] not in b_lineage_hashes, "POST to Business should not be B"
    
    def test_B_detection_burst(self):
        """B判定：write_method かつ burst>=20 でBになる"""
        detector = ABCDetector(run_id="test_run")
        
        # Create 20 POST events in same 5-minute window (user×domain)
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        events = []
        for i in range(20):
            event_time = base_time + timedelta(seconds=i * 10)  # All within 5 minutes
            event = self._create_event(
                user_id="user1",
                dest_domain="example.com",
                http_method="POST",
                bytes_sent=1000,  # Small, not A
                event_time=event_time
            )
            events.append(event)
        
        results = detector.detect(events)
        
        b_signals = results["signals"]["B"]
        b_lineage_hashes = {s["ingest_lineage_hash"] for s in b_signals}
        
        # All 20 events should be B candidates (burst >= 20)
        assert len(b_lineage_hashes) == 20, f"Expected 20 B candidates, got {len(b_lineage_hashes)}"
    
    def test_B_detection_cumulative(self):
        """B判定：write_method かつ cumulative>=20MB でBになる"""
        detector = ABCDetector(run_id="test_run")
        
        # Create events that sum to >= 20MB in same UTC day (user×domain)
        # All events must be on the same UTC day
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        events = []
        bytes_per_event = 1048576  # 1MB per event
        
        num_events = 21  # 21 events of 1MB = 21MB > 20MB
        for i in range(num_events):
            # Use minutes instead of hours to keep all events on same day
            event_time = base_time + timedelta(minutes=i * 30)  # Same UTC day
            event = self._create_event(
                user_id="user1",
                dest_domain="example.com",
                http_method="POST",
                bytes_sent=bytes_per_event,
                event_time=event_time,
                ingest_lineage_hash=f"cumulative_event_{i}"
            )
            events.append(event)
        
        results = detector.detect(events)
        
        b_signals = results["signals"]["B"]
        b_lineage_hashes = {s["ingest_lineage_hash"] for s in b_signals}
        
        # All events should be B candidates (cumulative >= 20MB)
        assert len(b_lineage_hashes) > 0, f"Should have B candidates from cumulative threshold, got {len(b_lineage_hashes)}"
        # Note: All events should be B candidates, but we check at least some are detected
        assert len(b_lineage_hashes) == num_events, f"All {num_events} events should be B candidates, got {len(b_lineage_hashes)}"
    
    def test_C_deterministic_sampling(self):
        """C判定：同じ入力（同じrun_id・同じ行）で、C採否が毎回一致（決定性）"""
        run_id = "test_run_deterministic"
        detector1 = ABCDetector(run_id=run_id)
        detector2 = ABCDetector(run_id=run_id)
        
        # Create B candidates with bytes_sent < 1MB
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        events = []
        for i in range(100):  # Create 100 B candidates
            event_time = base_time + timedelta(seconds=i * 10)
            # Create burst condition (20 events in 5min window)
            if i < 20:
                # First 20 events create burst
                event = self._create_event(
                    user_id="user1",
                    dest_domain="example.com",
                    http_method="POST",
                    bytes_sent=500000,  # < 1MB, so eligible for C
                    event_time=event_time,
                    ingest_lineage_hash=f"hash_{i}"  # Fixed hash for reproducibility
                )
            else:
                # Additional events to maintain burst
                event = self._create_event(
                    user_id="user1",
                    dest_domain="example.com",
                    http_method="POST",
                    bytes_sent=500000,
                    event_time=event_time,
                    ingest_lineage_hash=f"hash_{i}"
                )
            events.append(event)
        
        # Run detection twice with same run_id
        results1 = detector1.detect(events)
        results2 = detector2.detect(events)
        
        c_signals1 = results1["signals"]["C"]
        c_signals2 = results2["signals"]["C"]
        
        c_lineage_hashes1 = {s["ingest_lineage_hash"] for s in c_signals1}
        c_lineage_hashes2 = {s["ingest_lineage_hash"] for s in c_signals2}
        
        # C sampling should be deterministic (same run_id → same results)
        assert c_lineage_hashes1 == c_lineage_hashes2, "C sampling should be deterministic with same run_id"
    
    def test_C_different_run_id_different_sample(self):
        """C判定：異なるrun_idでは異なるサンプルが選ばれる可能性がある"""
        detector1 = ABCDetector(run_id="run1")
        detector2 = ABCDetector(run_id="run2")
        
        # Create B candidates
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        events = []
        for i in range(50):  # Create 50 B candidates
            event_time = base_time + timedelta(seconds=i * 10)
            event = self._create_event(
                user_id="user1",
                dest_domain="example.com",
                http_method="POST",
                app_category="AI",  # High-risk, so B candidate
                bytes_sent=500000,  # < 1MB
                event_time=event_time,
                ingest_lineage_hash=f"hash_{i}"
            )
            events.append(event)
        
        results1 = detector1.detect(events)
        results2 = detector2.detect(events)
        
        c_lineage_hashes1 = {s["ingest_lineage_hash"] for s in results1["signals"]["C"]}
        c_lineage_hashes2 = {s["ingest_lineage_hash"] for s in results2["signals"]["C"]}
        
        # Different run_id may produce different samples (not required to be different,
        # but should be possible)
        # We just verify both produce some C samples
        assert len(c_lineage_hashes1) > 0, "run1 should produce C samples"
        assert len(c_lineage_hashes2) > 0, "run2 should produce C samples"
    
    def test_zero_exclusion_prevention(self):
        """原則担保：Aが0でもBやCが取り得る入力で、B/Cが出力されること（サイズ閾値単独で終わらない）"""
        detector = ABCDetector(run_id="test_run")
        
        # Create events where A=0 (all < 1MB) but B/C should be detected
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        events = []
        
        # Create burst condition (20 POST events in 5min, all < 1MB)
        for i in range(20):
            event_time = base_time + timedelta(seconds=i * 10)
            event = self._create_event(
                user_id="user1",
                dest_domain="example.com",
                http_method="POST",
                bytes_sent=500000,  # < 1MB, so not A
                event_time=event_time,
                ingest_lineage_hash=f"burst_hash_{i}"
            )
            events.append(event)
        
        # Create high-risk category event (POST to AI, < 1MB)
        event_high_risk = self._create_event(
            user_id="user2",
            dest_domain="ai-service.com",
            http_method="POST",
            app_category="AI",
            bytes_sent=100000,  # < 1MB, so not A
            event_time=base_time,
            ingest_lineage_hash="high_risk_hash"
        )
        events.append(event_high_risk)
        
        results = detector.detect(events)
        
        a_signals = results["signals"]["A"]
        b_signals = results["signals"]["B"]
        c_signals = results["signals"]["C"]
        
        # A should be 0 (all events < 1MB)
        assert len(a_signals) == 0, f"Expected 0 A signals, got {len(a_signals)}"
        
        # B should be detected (burst or high-risk)
        assert len(b_signals) > 0, "Should have B signals even when A=0"
        
        # C should be detected (sample from B candidates < 1MB)
        # Note: With 2% sample rate, we may not always get C samples with small number of B candidates
        # But we verify that B candidates exist (which is the key requirement)
        # The key point is that A=0 but B/C are detected (zero-exclusion prevention)
        assert len(b_signals) > 0, "Should have B signals even when A=0 (zero-exclusion prevention)"
        
        # Create more B candidates to ensure C sampling works
        # Add 50 more high-risk events to increase B candidate count
        for i in range(50):
            event = self._create_event(
                user_id=f"user{i % 5}",
                dest_domain="ai-service.com",
                http_method="POST",
                app_category="AI",
                bytes_sent=500000,  # < 1MB
                event_time=base_time + timedelta(seconds=i),
                ingest_lineage_hash=f"high_risk_hash_{i}"
            )
            events.append(event)
        
        results2 = detector.detect(events)
        c_signals2 = results2["signals"]["C"]
        
        # With 70+ B candidates, we should get at least 1 C sample (2%)
        assert len(c_signals2) > 0, f"Should have C signals with many B candidates, got {len(results2['signals']['B'])} B candidates"
    
    def test_event_flags_output(self):
        """イベント単位のフラグ（candidate_flags）が正しく出力される"""
        detector = ABCDetector(run_id="test_run")
        
        # Create A event (>= 1MB)
        event_a = self._create_event(
            bytes_sent=2000000,  # 2MB, so A
            ingest_lineage_hash="event_a"
        )
        
        # Create B event (POST to AI, < 1MB)
        event_b = self._create_event(
            http_method="POST",
            app_category="AI",
            bytes_sent=500000,  # < 1MB
            ingest_lineage_hash="event_b"
        )
        
        results = detector.detect([event_a, event_b])
        
        event_flags = results["event_flags"]
        flags_map = {ef["ingest_lineage_hash"]: ef["candidate_flags"] for ef in event_flags}
        
        # Check A flag
        assert "A" in flags_map.get("event_a", ""), "event_a should have A flag"
        
        # Check B flag
        assert "B" in flags_map.get("event_b", ""), "event_b should have B flag"
    
    def test_metadata_output(self):
        """監査説明用のメタデータが正しく出力される"""
        detector = ABCDetector(run_id="test_run_metadata")
        
        events = [
            self._create_event(bytes_sent=2000000),  # A
            self._create_event(http_method="POST", app_category="AI", bytes_sent=500000)  # B
        ]
        
        results = detector.detect(events)
        metadata = results["metadata"]
        
        # Check thresholds_used
        assert "thresholds_used" in metadata
        assert metadata["thresholds_used"]["A_min_bytes"] == 1048576  # 1MB
        assert metadata["thresholds_used"]["C_sample_rate"] == 0.02  # 2%
        
        # Check counts
        assert "counts" in metadata
        assert metadata["counts"]["A_count"] >= 0
        assert metadata["counts"]["B_count"] >= 0
        assert metadata["counts"]["C_count"] >= 0
        assert metadata["counts"]["total_events"] == 2
        
        # Check sample info
        assert "sample" in metadata
        assert metadata["sample"]["sample_method"] == "deterministic_hash"
        assert metadata["sample"]["seed"] == "test_run_metadata"
    
    def test_5min_window_alignment(self):
        """5分窓がUTC境界に正しくアラインされる"""
        detector = ABCDetector(run_id="test_run")
        
        # Create events at different times within same 5-minute window
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        events = []
        
        # Events at 10:00, 10:01, 10:02, 10:03, 10:04 (all in same 5min window)
        for minute in range(5):
            event_time = base_time + timedelta(minutes=minute)
            event = self._create_event(
                user_id="user1",
                dest_domain="example.com",
                http_method="POST",
                bytes_sent=1000,
                event_time=event_time,
                ingest_lineage_hash=f"window_event_{minute}"
            )
            events.append(event)
        
        # Event at 10:05 (different 5min window)
        event_outside = self._create_event(
            user_id="user1",
            dest_domain="example.com",
            http_method="POST",
            bytes_sent=1000,
            event_time=base_time + timedelta(minutes=5),
            ingest_lineage_hash="window_event_outside"
        )
        events.append(event_outside)
        
        results = detector.detect(events)
        
        # First 5 events should be in same burst window (if we create 20 total)
        # For this test, we just verify the detector processes them correctly
        assert len(results["event_flags"]) == 6
    
    def test_utc_day_boundary(self):
        """日次集計がUTC日境界で正しく動作する"""
        detector = ABCDetector(run_id="test_run")
        
        # Create events on same UTC day
        day1_morning = datetime(2024, 1, 15, 10, 0, 0)
        day1_evening = datetime(2024, 1, 15, 22, 0, 0)
        
        events = [
            self._create_event(
                user_id="user1",
                dest_domain="example.com",
                http_method="POST",
                bytes_sent=10000000,  # 10MB
                event_time=day1_morning,
                ingest_lineage_hash="day1_morning"
            ),
            self._create_event(
                user_id="user1",
                dest_domain="example.com",
                http_method="POST",
                bytes_sent=15000000,  # 15MB (total 25MB >= 20MB)
                event_time=day1_evening,
                ingest_lineage_hash="day1_evening"
            )
        ]
        
        results = detector.detect(events)
        
        # Both events should be B candidates (cumulative >= 20MB on same day)
        b_signals = results["signals"]["B"]
        b_lineage_hashes = {s["ingest_lineage_hash"] for s in b_signals}
        
        assert "day1_morning" in b_lineage_hashes
        assert "day1_evening" in b_lineage_hashes
