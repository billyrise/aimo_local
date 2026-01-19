"""
AIMO Analysis Engine - Orchestrator Module

Provides orchestration utilities including file stabilization for Box sync.
"""

# Re-export Orchestrator and RunContext from parent orchestrator.py
# This allows: from orchestrator import Orchestrator, RunContext
# to work even though orchestrator.py is in the parent directory
import importlib.util
from pathlib import Path

# Load orchestrator.py from parent directory
orchestrator_file = Path(__file__).parent.parent / "orchestrator.py"
spec = importlib.util.spec_from_file_location("orchestrator_module", orchestrator_file)
orchestrator_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(orchestrator_module)

# Re-export
Orchestrator = orchestrator_module.Orchestrator
RunContext = orchestrator_module.RunContext

__all__ = ['Orchestrator', 'RunContext']
