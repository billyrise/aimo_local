"""
Tests for Phase 12: Token Bucket Budget Control with Priority Management

Tests priority-based budget control:
- A candidates: Always analyze (even if over budget)
- B candidates: Always analyze (even if over budget)
- C candidates: Skip if budget exhausted
"""

import pytest
from datetime import datetime
from src.llm.budget import BudgetController, CandidatePriority


class TestBudgetController:
    """Test budget controller with priority management."""
    
    def test_budget_initialization(self):
        """Test budget controller initialization."""
        controller = BudgetController(
            daily_limit_usd=10.0,
            priority_order=["A", "B", "C"],
            estimation_buffer=1.2
        )
        
        assert controller.daily_limit_usd == 10.0
        assert controller.priority_order == ["A", "B", "C"]
        assert controller.estimation_buffer == 1.2
        assert controller.daily_spent_usd == 0.0
    
    def test_extract_priority_A(self):
        """Test priority extraction for A candidates."""
        controller = BudgetController()
        
        # A flag should return highest priority
        priority = controller.extract_priority_from_flags("A|burst")
        assert priority == CandidatePriority.A
        
        priority = controller.extract_priority_from_flags("A")
        assert priority == CandidatePriority.A
    
    def test_extract_priority_B(self):
        """Test priority extraction for B candidates."""
        controller = BudgetController()
        
        # B flag should return B priority
        priority = controller.extract_priority_from_flags("B|cumulative")
        assert priority == CandidatePriority.B
        
        priority = controller.extract_priority_from_flags("B")
        assert priority == CandidatePriority.B
    
    def test_extract_priority_C(self):
        """Test priority extraction for C candidates."""
        controller = BudgetController()
        
        # C flag should return C priority
        priority = controller.extract_priority_from_flags("C|sampled")
        assert priority == CandidatePriority.C
        
        priority = controller.extract_priority_from_flags("C")
        assert priority == CandidatePriority.C
    
    def test_extract_priority_mixed(self):
        """Test priority extraction with mixed flags (A takes precedence)."""
        controller = BudgetController()
        
        # A should take precedence over B and C
        priority = controller.extract_priority_from_flags("A|B|C")
        assert priority == CandidatePriority.A
        
        # B should take precedence over C
        priority = controller.extract_priority_from_flags("B|C")
        assert priority == CandidatePriority.B
    
    def test_extract_priority_none(self):
        """Test priority extraction with no priority flags."""
        controller = BudgetController()
        
        # No priority flags should return None
        priority = controller.extract_priority_from_flags("burst|cumulative")
        assert priority is None
        
        priority = controller.extract_priority_from_flags(None)
        assert priority is None
    
    def test_should_analyze_A_always(self):
        """Test that A candidates are always analyzed, even if over budget."""
        controller = BudgetController(daily_limit_usd=10.0)
        
        # Spend all budget
        controller.record_spending(10.0)
        assert controller.daily_spent_usd == 10.0
        
        # A candidate should still be analyzed (even if over budget)
        should_analyze, reason = controller.should_analyze(5.0, "A|burst")
        assert should_analyze is True
        assert "priority_A_always_analyze" in reason
    
    def test_should_analyze_B_always(self):
        """Test that B candidates are always analyzed, even if over budget."""
        controller = BudgetController(daily_limit_usd=10.0)
        
        # Spend all budget
        controller.record_spending(10.0)
        assert controller.daily_spent_usd == 10.0
        
        # B candidate should still be analyzed (even if over budget)
        should_analyze, reason = controller.should_analyze(5.0, "B|cumulative")
        assert should_analyze is True
        assert "priority_B_always_analyze" in reason
    
    def test_should_analyze_C_with_budget(self):
        """Test that C candidates are analyzed if budget is available."""
        controller = BudgetController(daily_limit_usd=10.0)
        
        # Spend some budget
        controller.record_spending(5.0)
        assert controller.daily_spent_usd == 5.0
        
        # C candidate should be analyzed if budget available
        should_analyze, reason = controller.should_analyze(3.0, "C|sampled")
        assert should_analyze is True
        assert "priority_C_budget_available" in reason
    
    def test_should_analyze_C_budget_exhausted(self):
        """Test that C candidates are skipped if budget is exhausted."""
        controller = BudgetController(daily_limit_usd=10.0)
        
        # Spend all budget
        controller.record_spending(10.0)
        assert controller.daily_spent_usd == 10.0
        
        # C candidate should be skipped if budget exhausted
        should_analyze, reason = controller.should_analyze(1.0, "C|sampled")
        assert should_analyze is False
        assert "priority_C_budget_exhausted" in reason
    
    def test_should_analyze_no_priority_with_budget(self):
        """Test signatures without priority flags with available budget."""
        controller = BudgetController(daily_limit_usd=10.0)
        
        # Spend some budget
        controller.record_spending(5.0)
        
        # No priority flags, but budget available -> analyze
        should_analyze, reason = controller.should_analyze(3.0, "burst|cumulative")
        assert should_analyze is True
        assert "no_priority_flags_budget_available" in reason
    
    def test_should_analyze_no_priority_budget_exhausted(self):
        """Test signatures without priority flags with exhausted budget."""
        controller = BudgetController(daily_limit_usd=10.0)
        
        # Spend all budget
        controller.record_spending(10.0)
        
        # No priority flags, budget exhausted -> skip
        should_analyze, reason = controller.should_analyze(1.0, "burst|cumulative")
        assert should_analyze is False
        assert "no_priority_flags_budget_exhausted" in reason
    
    def test_filter_by_priority(self):
        """Test filtering signatures by priority."""
        controller = BudgetController(daily_limit_usd=10.0)
        
        # Spend most of budget
        controller.record_spending(9.0)
        
        signatures = [
            {"url_signature": "sig1", "candidate_flags": "A|burst"},
            {"url_signature": "sig2", "candidate_flags": "B|cumulative"},
            {"url_signature": "sig3", "candidate_flags": "C|sampled"},
            {"url_signature": "sig4", "candidate_flags": None},
        ]
        
        estimated_cost_per_sig = 1.0
        to_analyze, skipped = controller.filter_by_priority(signatures, estimated_cost_per_sig)
        
        # A and B should be analyzed (always)
        assert len(to_analyze) == 2
        assert any(sig["url_signature"] == "sig1" for sig in to_analyze)
        assert any(sig["url_signature"] == "sig2" for sig in to_analyze)
        
        # C and no-priority should be skipped (budget exhausted)
        assert len(skipped) == 2
        assert any(sig["url_signature"] == "sig3" for sig in skipped)
        assert any(sig["url_signature"] == "sig4" for sig in skipped)
        
        # Check skip reasons
        sig3_skipped = next(sig for sig in skipped if sig["url_signature"] == "sig3")
        assert "priority_C_budget_exhausted" in sig3_skipped["skip_reason"]
    
    def test_budget_status(self):
        """Test budget status reporting."""
        controller = BudgetController(daily_limit_usd=10.0)
        
        # Spend some budget
        controller.record_spending(3.0)
        
        status = controller.get_status()
        
        assert status["daily_limit_usd"] == 10.0
        assert status["daily_spent_usd"] == 3.0
        assert status["remaining_usd"] == 7.0
        assert status["utilization"] == 0.3
        assert "reset_date" in status
        assert status["priority_order"] == ["A", "B", "C"]
    
    def test_budget_reset_on_new_day(self):
        """Test that budget resets on new day."""
        controller = BudgetController(daily_limit_usd=10.0)
        
        # Spend budget
        controller.record_spending(10.0)
        assert controller.daily_spent_usd == 10.0
        
        # Manually set reset date to yesterday (simulate day change)
        from datetime import timedelta
        controller.daily_reset_date = datetime.utcnow().date() - timedelta(days=1)
        
        # Reset should occur
        controller.reset_if_new_day()
        assert controller.daily_spent_usd == 0.0
        assert controller.daily_reset_date == datetime.utcnow().date()
    
    def test_priority_order_custom(self):
        """Test custom priority order."""
        controller = BudgetController(
            daily_limit_usd=10.0,
            priority_order=["B", "A", "C"]  # Custom order
        )
        
        # B should still be extracted correctly
        priority = controller.extract_priority_from_flags("B|cumulative")
        assert priority == CandidatePriority.B
        
        # But priority order in status should reflect custom order
        status = controller.get_status()
        assert status["priority_order"] == ["B", "A", "C"]
