"""
Token Bucket Budget Control with Priority Management

Implements priority-based budget control for LLM analysis:
- A candidates: Always analyze (high-volume = high risk)
- B candidates: Always analyze (burst/cumulative = suspicious)
- C candidates: Skip if budget exhausted (coverage sample)
"""

from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date
from enum import Enum


class CandidatePriority(Enum):
    """Candidate priority levels for budget control."""
    A = 1  # Highest priority: always analyze
    B = 2  # High priority: always analyze
    C = 3  # Low priority: skip if budget exhausted


class BudgetController:
    """
    Budget controller with priority-based enforcement.
    
    Features:
    - Daily budget tracking with automatic reset
    - Priority-based request filtering (A/B/C)
    - Cost estimation with buffer
    - Budget exhaustion detection
    """
    
    def __init__(self, 
                 daily_limit_usd: float = 10.0,
                 priority_order: Optional[List[str]] = None,
                 estimation_buffer: float = 1.2):
        """
        Initialize budget controller.
        
        Args:
            daily_limit_usd: Daily budget limit in USD
            priority_order: Priority order (e.g., ["A", "B", "C"])
            estimation_buffer: Cost estimation buffer multiplier
        """
        self.daily_limit_usd = daily_limit_usd
        self.priority_order = priority_order or ["A", "B", "C"]
        self.estimation_buffer = estimation_buffer
        
        # Daily tracking
        self.daily_spent_usd = 0.0
        self.daily_reset_date = datetime.utcnow().date()
        
        # Priority mapping
        self.priority_map = {
            "A": CandidatePriority.A,
            "B": CandidatePriority.B,
            "C": CandidatePriority.C
        }
    
    def reset_if_new_day(self) -> bool:
        """
        Reset daily spending if date changed.
        
        Returns:
            True if reset occurred, False otherwise
        """
        today = datetime.utcnow().date()
        if today > self.daily_reset_date:
            self.daily_spent_usd = 0.0
            self.daily_reset_date = today
            return True
        return False
    
    def get_remaining_budget(self) -> float:
        """
        Get remaining daily budget.
        
        Returns:
            Remaining budget in USD
        """
        self.reset_if_new_day()
        return max(0.0, self.daily_limit_usd - self.daily_spent_usd)
    
    def get_budget_utilization(self) -> float:
        """
        Get current budget utilization (0.0 to 1.0).
        
        Returns:
            Budget utilization ratio
        """
        self.reset_if_new_day()
        if self.daily_limit_usd == 0:
            return 0.0
        return min(1.0, self.daily_spent_usd / self.daily_limit_usd)
    
    def can_afford(self, estimated_cost_usd: float) -> bool:
        """
        Check if estimated cost can be afforded.
        
        Args:
            estimated_cost_usd: Estimated cost for request
        
        Returns:
            True if within budget, False otherwise
        """
        self.reset_if_new_day()
        return (self.daily_spent_usd + estimated_cost_usd) <= self.daily_limit_usd
    
    def record_spending(self, actual_cost_usd: float) -> None:
        """
        Record actual spending.
        
        Args:
            actual_cost_usd: Actual cost incurred
        """
        self.reset_if_new_day()
        self.daily_spent_usd += actual_cost_usd
    
    def extract_priority_from_flags(self, candidate_flags: Optional[str]) -> Optional[CandidatePriority]:
        """
        Extract priority from candidate_flags string.
        
        Args:
            candidate_flags: Pipe-separated flags (e.g., "A|B|burst")
        
        Returns:
            CandidatePriority or None if no priority flags found
        """
        if not candidate_flags:
            return None
        
        flags = candidate_flags.split("|")
        
        # Check in priority order (A > B > C)
        for priority_str in self.priority_order:
            if priority_str in flags:
                return self.priority_map.get(priority_str)
        
        return None
    
    def should_analyze(self, 
                      estimated_cost_usd: float,
                      candidate_flags: Optional[str] = None) -> Tuple[bool, str]:
        """
        Determine if signature should be analyzed based on priority and budget.
        
        Args:
            estimated_cost_usd: Estimated cost for this request
            candidate_flags: Pipe-separated flags (e.g., "A|B|burst")
        
        Returns:
            Tuple of (should_analyze: bool, reason: str)
        """
        self.reset_if_new_day()
        
        # Extract priority from flags
        priority = self.extract_priority_from_flags(candidate_flags)
        
        # If no priority flags, treat as low priority (skip if budget exhausted)
        if priority is None:
            if self.can_afford(estimated_cost_usd):
                return True, "no_priority_flags_budget_available"
            else:
                return False, "no_priority_flags_budget_exhausted"
        
        # Priority-based decision
        if priority == CandidatePriority.A:
            # A candidates: always analyze (even if over budget)
            # This ensures high-volume transfers are never skipped
            return True, "priority_A_always_analyze"
        
        elif priority == CandidatePriority.B:
            # B candidates: always analyze (even if over budget)
            # This ensures high-risk small transfers are never skipped
            return True, "priority_B_always_analyze"
        
        elif priority == CandidatePriority.C:
            # C candidates: only analyze if budget available
            if self.can_afford(estimated_cost_usd):
                return True, "priority_C_budget_available"
            else:
                return False, "priority_C_budget_exhausted"
        
        # Fallback: skip if budget exhausted
        if self.can_afford(estimated_cost_usd):
            return True, "fallback_budget_available"
        else:
            return False, "fallback_budget_exhausted"
    
    def filter_by_priority(self,
                          signatures: List[Dict[str, Any]],
                          estimated_cost_per_signature: float) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Filter signatures by priority and budget.
        
        Args:
            signatures: List of signature dicts with candidate_flags
            estimated_cost_per_signature: Estimated cost per signature
        
        Returns:
            Tuple of (to_analyze: List, skipped: List)
        """
        to_analyze = []
        skipped = []
        
        for sig in signatures:
            candidate_flags = sig.get("candidate_flags")
            estimated_cost = estimated_cost_per_signature
            
            should_analyze, reason = self.should_analyze(estimated_cost, candidate_flags)
            
            if should_analyze:
                to_analyze.append(sig)
            else:
                skipped_sig = sig.copy()
                skipped_sig["skip_reason"] = reason
                skipped.append(skipped_sig)
        
        return to_analyze, skipped
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get current budget status.
        
        Returns:
            Dict with budget status information
        """
        self.reset_if_new_day()
        remaining = self.get_remaining_budget()
        utilization = self.get_budget_utilization()
        
        return {
            "daily_limit_usd": self.daily_limit_usd,
            "daily_spent_usd": self.daily_spent_usd,
            "remaining_usd": remaining,
            "utilization": utilization,
            "reset_date": self.daily_reset_date.isoformat(),
            "priority_order": self.priority_order
        }
