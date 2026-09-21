"""
Module: discovery
Description: Target Discovery Data Products and Evidence Mart Layer.
             Normalizes and aggregates multi-omics ClinVar variant observations and OMOP CDM
             clinical diagnosis histories to evaluate target tractability, disease association
             odds ratios, mutation burdens, and GxP contract enforcement.
Author: Vivi Tsoumaki
"""

from discovery.target_mart import (
    TargetEvidenceMart,
    TargetEvidenceMartConfig,
    validate_and_quarantine_target_records,
)

__all__ = [
    "TargetEvidenceMart",
    "TargetEvidenceMartConfig",
    "validate_and_quarantine_target_records",
]
