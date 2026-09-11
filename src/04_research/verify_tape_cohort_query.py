"""Independent artifact verifier and tiny normative state-oracle entry point."""
from __future__ import annotations
import argparse
import json
from pathlib import Path

from tape_cohort_config import normalize_config
from tape_cohort_outputs import verify_date
from tape_cohort_state import CohortMachine


def worked_transition_oracle():
    config=normalize_config({"schema":"tape_cohort_config_v1","semantics_version":"tape_cohort_hysteresis_v1",
        "eligibility":"post_discovery_and_requested_zero_masks_v1","decision_session":"extended_0400_2000_ET",
        "entry_confirm_seconds":3,"exit_confirm_seconds":3,"conditions":[{"feature":"trade_rate_60s","unit":"trades/second",
        "entry":{"lower":2.0,"lower_inclusive":True,"upper":None,"upper_inclusive":True},
        "continuation":{"lower":1.0,"lower_inclusive":True,"upper":None,"upper_inclusive":True}}]})
    sequence=[(1,1),(1,1),(0,1),(1,1),(1,1),(1,1),(0,1),(0,0),(0,1),(0,0),(0,0),(0,0)]
    sinks={"windows":[],"window_features":[],"strict_runs":[]}; machine=CohortMachine(config,"0"*64)
    for t,(strict,continuation) in enumerate(sequence,1):
        value=2.0 if strict else 1.0 if continuation else 0.0
        machine.consume_row({"interval_end_ns":t*1_000_000_000,"continuity_segment_id":1,"halt_interval_active":False,
            "post_discovery_eligible":True,"trade_rate_60s":value,"trade_rate_60s_reason_mask":0},sinks)
    machine.finish("selection_boundary",sinks)
    if len(sinks["windows"])!=1: raise AssertionError("worked oracle window count")
    w=sinks["windows"][0]
    expected={"candidate_start_endpoint_ns":4_000_000_000,"entry_confirmed_at_ns":6_000_000_000,
              "exit_trigger_endpoint_ns":10_000_000_000,"exit_effective_at_ns":12_000_000_000,
              "active_seconds":6,"strict_pass_count":1,"continuation_pass_count":3,"pending_exit_count":3}
    if any(w[k]!=v for k,v in expected.items()): raise AssertionError({k:(w[k],v) for k,v in expected.items() if w[k]!=v})
    return expected


def verify_run(run):
    root=Path(run); manifest=json.loads((root/"manifest.json").read_text())
    if manifest.get("state")!="complete": raise ValueError("run is incomplete")
    verified=[]
    if (root/"dates").exists():
        for day in sorted((root/"dates").iterdir()):
            date_manifest=json.loads((day/"manifest.json").read_text())
            verified.append({"date":day.name,**verify_date(day,date_manifest,len(json.loads((root/"query.json").read_text())["conditions"]))})
    return {"state":"verified","dates":verified,"worked_oracle":worked_transition_oracle()}


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("--run",type=Path);p.add_argument("--oracle",action="store_true");a=p.parse_args()
    print(json.dumps(worked_transition_oracle() if a.oracle else verify_run(a.run),indent=2))
if __name__=="__main__": main()
