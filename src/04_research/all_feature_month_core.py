"""Exact, bounded EDA extensions; no changes to the incumbent feature engine.

O(Q + N*H) time, O(B*F + H*F + output batch) memory. Raw/base batches
<=4096 by default (hard ceiling 25000), output batches <=1024, H<=300.
Movement mean comparison: rtol=1e-10, atol=1e-12 bps; counts are exact.
"""
from __future__ import annotations

from collections import deque, Counter
from functools import lru_cache
from datetime import datetime
from itertools import zip_longest
import json
import hashlib
import math
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src/03_features'))
import economic_tape_state_v3 as V

from all_feature_month_schema import (VERSION, HORIZONS, REASONS, MAPPINGS, FEATURES,
    REGISTRY, field_metadata, schema as explicit_schema, NUMERICAL_POLICY)
from all_feature_month_moments import Moments
AGE_VERSION = 'exact_midpoint_change_age_status_reset_v2'
NS = V.NS
BASE_DIAGNOSTICS = ('state_observed_second_count', 'state_mature', 'movement_valid_5s_count',
    'movement_valid_1s_count', 'movement_support_valid', 'activity_valid_second_count',
    'activity_support_valid', 'quote_age_observation_count', 'trade_age_observation_count',
    'quoted_spread_valid', 'quoted_spread_valid_fraction', 'state_fully_post_halt',
    'state_pre_halt_observation_fraction', 'state_contains_pre_halt_history',
    'state_post_halt_observed_seconds', 'state_carried_forward_during_halt')
KEYS = ('session_date', 'symbol', 'interval_end_ns')
CONTEXT = (*KEYS, 'midpoint', 'continuity_segment_id', 'halt_interval_active', 'halt_interval_id',
    'historical_ex_post_overlay', 'live_reproducible', 'seconds_since_halt_resume',
    'primitive_quote_source_file_accepted', 'primitive_trade_source_file_accepted')
BASE_COLUMNS = list(CONTEXT) + ['primitive_trade_count','primitive_dollars','primitive_trade_age','primitive_quote_age'] + [f'{x}_{h}s' for h in HORIZONS for x in
    (*BASE_DIAGNOSTICS, *(v[0] for v in MAPPINGS.values()), 'movement_to_spread')]
# The quote/movement pass does not consume copied activity/age summaries or
# assembly provenance. Project only what its two reducers actually read.
EXTENSION_BASE_COLUMNS=list(KEYS)+['midpoint','continuity_segment_id','halt_interval_active',
    'primitive_quote_source_file_accepted','primitive_quote_age']+[f'{x}_{h}s' for h in HORIZONS for x in (
    'movement_valid_5s_count','movement_valid_1s_count','state_observed_second_count',
    'state_mature','movement_support_valid','midpoint_movement_bps_per_30s')]


def schema_hash(schema):
    return hashlib.sha256(schema.remove_metadata().serialize().to_pybytes()).hexdigest()


def finite(x):
    return x is not None and math.isfinite(x)


def equal(a, b, label):
    if not finite(a) and not finite(b):
        return
    if not finite(a) or not finite(b) or not math.isclose(a, b, rel_tol=1e-10, abs_tol=1e-12):
        raise ValueError(f'{label} reconstruction mismatch: {a!r} != {b!r}')


def rows(path, columns=None, batch_size=4096, check=lambda: None, limit=None, validate_finite=False):
    if not 1 <= batch_size <= 25000:
        raise ValueError('batch size must be 1..25000')
    pf = pq.ParquetFile(path)
    if columns and set(columns) - set(pf.schema_arrow.names):
        raise ValueError('missing required published metadata: '+str(sorted(set(columns)-set(pf.schema_arrow.names))))
    remaining=pf.metadata.num_rows if limit is None else limit
    for batch in pf.iter_batches(batch_size=batch_size, columns=columns, use_threads=False):
        check()
        take=min(remaining,batch.num_rows)
        selected=batch.slice(0,take)
        if validate_finite:
            for field,column in zip(selected.schema,selected.columns):
                if pa.types.is_floating(field.type) and not pc.all(pc.fill_null(pc.is_finite(column),True)).as_py():
                    raise ValueError('nonfinite persisted value: '+field.name)
        yield from selected.to_pylist()
        remaining-=take
        if remaining==0:break
    if remaining:raise ValueError('base prefix incomplete')


class SupportCount:
    """Fixed rolling support slots; no arithmetic on unused return magnitudes."""
    def __init__(self, capacity):
        self.capacity=capacity;self.queue=deque();self.count=0
    def append(self, value):
        if len(self.queue)==self.capacity:self.count-=self.queue.popleft()
        valid=finite(value);self.queue.append(valid);self.count+=valid


class Movement:
    """Reconstruct the inherited paused rolling clock from published rows.

    Publication includes source acceptance. Generation changes only on halt
    resumption; clearing the six endpoints at every halt gives the same lag
    eligibility without exposing the private generation counter. Continuity
    changes and clock gaps clear all rolling state, as in FeatureStream.
    """
    def __init__(self):
        self.previous = self.epoch = None
        self.was_halt = False
        self.generation = 0
        self.reset()

    def reset(self):
        self.ends = deque(maxlen=6)
        self.windows = {h: Moments(h-4) for h in HORIZONS}
        self.ones = {h: SupportCount(h) for h in HORIZONS}
        self.slots = 0

    def push(self, row):
        t, epoch = row['interval_end_ns'], row['continuity_segment_id']
        if self.previous is not None:
            if t <= self.previous:
                raise ValueError('duplicate or unordered feature clock')
            if t != self.previous+NS or epoch != self.epoch:
                self.reset()
        self.previous, self.epoch = t, epoch
        active = row['halt_interval_active']
        resume = self.was_halt and not active
        if resume:self.generation += 1
        five = one = math.nan
        if active or self.was_halt:
            self.ends.clear()
        if not active:
            self.slots += 1
            self.ends.append((row['midpoint'], row['primitive_quote_source_file_accepted']))
            def change(lag):
                if len(self.ends) <= lag:
                    return math.nan
                history = list(self.ends)[-lag-1:]
                a, b = history[0][0], history[-1][0]
                if not all(x[1] for x in history) or not finite(a) or not finite(b) or min(a,b) <= 0:
                    return math.nan
                return abs(10000*math.log(b/a))
            five, one = change(5), change(1)
            for h in HORIZONS:
                self.windows[h].append(five)
                self.ones[h].append(one)
        result = dict(movement_5s_bps=five,movement_5s_valid=finite(five),movement_1s_valid=finite(one),
                      halt_resume_boundary=resume,halt_generation=self.generation)
        for h in HORIZONS:
            w = self.windows[h]
            if row[f'movement_valid_5s_count_{h}s'] != w.count or row[f'movement_valid_1s_count_{h}s'] != self.ones[h].count:
                raise ValueError('movement support reconstruction mismatch')
            if row[f'state_observed_second_count_{h}s'] != min(h,self.slots):
                raise ValueError('movement horizon reconstruction mismatch')
            count=min(h,self.slots)
            expected_support=count>=math.ceil(.8*h) and w.count>=math.ceil(.8*(h-4)) and self.ones[h].count>=math.ceil(.8*h)
            if row[f'state_mature_{h}s']!=(count==h) or row[f'movement_support_valid_{h}s']!=expected_support:
                raise ValueError('movement support/maturity gate reconstruction mismatch')
            equal(w.mean(), None if row[f'midpoint_movement_bps_per_30s_{h}s'] is None else
                  row[f'midpoint_movement_bps_per_30s_{h}s']/6, 'movement mean')
            result[f'movement_participation_{h}s'] = w.participation() if count==h and expected_support else math.nan
            result[f'movement_zero_total_{h}s'] = w.count>0 and w.positive==0
        self.was_halt = active
        return result


class MidpointAge:
    """One decoder/cursor integrates quote support and observes exact changes."""
    def __init__(self, events, start, check=lambda: None):
        self.cursor=V.Cursor(events);self.start=start;self.check=check
        self.previous_end=self.epoch=None;self.was_halt=False;self.events_consumed=0;self.source_lost=False
        self.reset()
        while self.cursor.current and self.cursor.current[0]<start:
            ts,event=self.pop()
            if ts>=start-300*NS:
                self.current=event;self.latest_quote=ts
                self.mid=event['midpoint'] if event['price_state_valid'] else None
        if self.mid is not None:self.origin=start

    def pop(self):
        if self.events_consumed%4096==0:self.check()
        self.events_consumed+=1
        return self.cursor.pop()

    def reset(self):
        self.current=self.latest_quote=self.mid=self.last_change=self.origin=None
        self.windows={h:V.RollingValues(h) for h in HORIZONS};self.elapsed=0

    def begin_interval(self,row):
        t,epoch=row['interval_end_ns'],row['continuity_segment_id'];left=t-NS
        active=row['halt_interval_active']
        gap=self.previous_end is not None and (t!=self.previous_end+NS or epoch!=self.epoch)
        if self.previous_end is not None and t<=self.previous_end:raise ValueError('duplicate or unordered age clock')
        if gap or active or self.was_halt:self.reset()
        if self.source_lost and row['primitive_quote_source_file_accepted'] and self.current and self.current['price_state_valid']:
            self.mid=self.current['midpoint'];self.origin=left
        self.begin_integrals(row)

    def push(self,row):
        self.begin_interval(row)
        t=row['interval_end_ns']
        while self.cursor.current and self.cursor.current[0]<t:
            ts,event=self.pop()
            self.consume_event(ts,event)
        return self.end_interval(row)

    def begin_integrals(self,row):
        self.interval_row=row
        self.duration=self.spread_duration=0
        self.spread_mass=0.
        self.midpoint_integral=V.MidpointIntegral()
        self.last=row['interval_end_ns']-NS

    def integrate(self,a,b):
        if self.current is None or self.interval_row['halt_interval_active'] or b<=a:return
        if self.current['price_state_valid']:
            dt=b-a;self.duration+=dt
            self.midpoint_integral.add(self.current['midpoint'],dt)
            if not self.current.get('locked',False):
                self.spread_duration+=dt;self.spread_mass+=(dt/NS)*self.current.get('spread_bps',0.)

    def consume_event(self,ts,event):
        """Shared event reducer; called by replay or the versioned builder tap."""
        row=self.interval_row;left=row['interval_end_ns']-NS
        if row['halt_interval_active'] or ts<left:return
        self.integrate(self.last,ts);self.last=ts
        valid=event['price_state_valid'] and finite(event['midpoint']) and event['midpoint']>0
        if not valid:self.mid=self.last_change=self.origin=None
        else:
            value=event['midpoint']
            if self.mid is None:self.origin=ts
            elif value!=self.mid:self.last_change=ts
            self.mid=value
        self.current=event;self.latest_quote=ts

    def end_interval(self,row):
        t,epoch=row['interval_end_ns'],row['continuity_segment_id'];active=row['halt_interval_active']
        self.integrate(self.last,t)
        duration,spread_duration,mass,spread_mass=self.duration,self.spread_duration,self.midpoint_integral.mass(),self.spread_mass
        if not 0<=spread_duration<=duration<=NS or not finite(mass) or not finite(spread_mass) or mass<0 or spread_mass<0:
            raise ValueError('invalid quote primitive integration')
        reconstructed=self.midpoint_integral.mean() if duration else None
        if 'midpoint' in row:equal(reconstructed,row['midpoint'],'quote midpoint')
        accepted=row['primitive_quote_source_file_accepted']
        raw_age=(t-self.latest_quote)/1e6 if self.latest_quote is not None and self.current and self.current['price_state_valid'] else None
        if 'primitive_quote_age' in row:equal(raw_age,row['primitive_quote_age'],'quote endpoint age')
        if not accepted:
            self.mid=self.last_change=self.origin=None
        self.source_lost=not accepted
        observable=accepted and not active and self.mid is not None
        age=(t-self.last_change)/NS if observable and self.last_change is not None else math.nan
        status='known' if finite(age) else 'no_change_observed' if observable else 'unobservable'
        if not active:
            self.elapsed+=1
            for w in self.windows.values():w.append(age)
        out=dict(midpoint_change_age_end_seconds=age,midpoint_age_observation_status=status,
            midpoint_observation_start_ns=self.origin if observable else None,
            midpoint_no_change_observed_seconds=(t-self.origin)/NS if status=='no_change_observed' else None,
            midpoint_valid_duration_ns=duration,quoted_spread_integral_bps_seconds=spread_mass,
            quoted_spread_valid_duration_ns=spread_duration)
        for h,w in self.windows.items():
            mature=self.elapsed>=h and not active;supported=w.count>=math.ceil(.8*h)
            out[f'midpoint_change_age_observation_count_{h}s']=w.count
            out[f'midpoint_change_age_mature_{h}s']=mature
            out[f'midpoint_change_age_support_valid_{h}s']=supported
            out[f'midpoint_change_age_p90_seconds_{h}s']=w.quantile(.9) if mature and supported and accepted else math.nan
        self.previous_end,self.epoch,self.was_halt=t,epoch,active
        return out

    def finish(self):
        while self.cursor.current:self.pop()


@lru_cache(maxsize=8)
def session_start(day):
    return V.session_bounds(day)[0]

FEATURES_BY_HORIZON={h:tuple(f for f in FEATURES if f.endswith(f'_{h}s')) for h in HORIZONS}
FAMILIES=tuple((name,v[3]) for name,v in REGISTRY.items() if v[3]!='ratio')
SUPPORT_STEMS={'movement':'movement_support_valid','spread':'quoted_spread_valid',
               'activity':'activity_support_valid','mid_age':'midpoint_change_age_support_valid'}


def quality(row, h, family, value):
    active = row['halt_interval_active']
    source = row['primitive_trade_source_file_accepted' if family in ('activity','trade_age') else 'primitive_quote_source_file_accepted']
    mature = row[f'midpoint_change_age_mature_{h}s'] if family == 'mid_age' else row[f'state_mature_{h}s']
    if family in ('trade_age','quote_age'):
        support=row[f'{family}_observation_count_{h}s'] >= math.ceil(.8*h)
    else:
        stem=SUPPORT_STEMS[family]
        support=row[f'{stem}_{h}s']
    return ((REASONS['undefined'] if not finite(value) else 0) |
            (REASONS['source_unaccepted'] if not source else 0) |
            (REASONS['immature'] if not mature else 0) |
            (REASONS['insufficient_support'] if not support else 0) |
            (REASONS['active_halt'] if active else 0))


def assemble(base, extension, discovery):
    if tuple(base[k] for k in KEYS) != tuple(extension[k] for k in KEYS):
        raise ValueError('one-to-one extension join key mismatch')
    row = {k:base[k] for k in CONTEXT}
    row.update({f'{x}_{h}s':base[f'{x}_{h}s'] for h in HORIZONS for x in BASE_DIAGNOSTICS})
    row.update(extension)
    count=base['primitive_trade_count'];dollars=base['primitive_dollars']
    if not finite(count) or count<0 or count>=2**63 or int(count)!=count or not finite(dollars) or dollars<0:
        raise ValueError('invalid transaction primitive')
    row['trade_count_1s']=int(count);row['dollar_volume_1s']=dollars
    for kind in ('trade','quote'):
        value=base[f'primitive_{kind}_age']
        row[f'{kind}_age_end_seconds']=value/1000 if finite(value) and not row['halt_interval_active'] and row[f'primitive_{kind}_source_file_accepted'] else None
    row['first_discovery_endpoint_ns'] = discovery.get('endpoint_ns')
    row['discovery_received_at_ns'] = discovery.get('received_at_ns')
    row['discovery_timing_basis'] = discovery.get('timing_basis', 'unavailable')
    row['discovery_provenance_hash'] = discovery.get('provenance_hash')
    row['post_discovery_eligible'] = bool(discovery.get('verified') and discovery.get('endpoint_ns') is not None
        and row['interval_end_ns'] >= discovery['endpoint_ns'])
    left = (row['interval_end_ns']-session_start(row['session_date']))//NS-1
    row['session_segment'] = 'premarket' if left < 19800 else 'rth' if left < 43200 else 'after_hours'
    for h in HORIZONS:
        for target,(source,scale,unit,family) in MAPPINGS.items():
            value = base[f'{source}_{h}s']
            # Division expresses the contract exactly, avoiding reciprocal rounding.
            row[f'{target}_{h}s'] = value/6 if target == 'movement_mean_5s_bps' and finite(value) else (
                value/1000 if 'age_p90' in target and finite(value) else value)
        for name,family in FAMILIES:
            f = f'{name}_{h}s'
            reason = quality(row,h,family,row[f])
            if name=='movement_participation' and row[f'movement_zero_total_{h}s']:
                reason |= REASONS['zero_total_movement']
            row[f+'_reason_mask'] = reason
            row[f+'_analysis_valid'] = reason == 0
        spread = row[f'quoted_spread_mean_bps_{h}s']
        for stat in ('mean',):
            numerator = f'movement_{stat}_5s_bps_{h}s'
            f = f'movement_{stat}_to_spread_{h}s'
            value = row[numerator]/spread if finite(row[numerator]) and finite(spread) and spread > 0 else None
            row[f] = value
            reason = row[numerator+'_reason_mask'] | row[f'quoted_spread_mean_bps_{h}s_reason_mask']
            if not finite(spread) or spread <= 0:
                reason |= REASONS['nonpositive_spread']
            if not finite(value):
                reason |= REASONS['undefined']
            row[f+'_reason_mask'], row[f+'_analysis_valid'] = reason, reason == 0
        published = base[f'movement_to_spread_{h}s']
        equal(row[f'movement_mean_to_spread_{h}s'], published/6 if finite(published) else None, 'mean ratio')
        for f in FEATURES_BY_HORIZON[h]:
            carried = not row[f'state_fully_post_halt_{h}s'] and not f.startswith('midpoint_change_age')
            if carried:
                row[f+'_reason_mask'] |= REASONS['carried_history']
            row[f+'_eda_eligible'] = bool(row[f+'_analysis_valid'] and not carried and row['post_discovery_eligible'])
    return {k:None if isinstance(v,float) and not math.isfinite(v) else v for k,v in row.items()}


def extension_columns():
    return list(KEYS)+['movement_5s_bps','movement_5s_valid','movement_1s_valid','halt_resume_boundary','halt_generation']+[
        f'{name}_{h}s' for h in HORIZONS for name in ('movement_participation','movement_zero_total')]+[
        'midpoint_change_age_end_seconds','midpoint_age_observation_status','midpoint_observation_start_ns',
        'midpoint_no_change_observed_seconds','midpoint_valid_duration_ns','quoted_spread_integral_bps_seconds',
        'quoted_spread_valid_duration_ns']+[f'{name}_{h}s' for h in HORIZONS for name in (
        'midpoint_change_age_observation_count','midpoint_change_age_mature','midpoint_change_age_support_valid','midpoint_change_age_p90_seconds')]


def final_columns():
    return list(dict.fromkeys([*CONTEXT,*[f'{x}_{h}s' for h in HORIZONS for x in BASE_DIAGNOSTICS],
        *extension_columns(),'trade_count_1s','dollar_volume_1s','trade_age_end_seconds','quote_age_end_seconds',
        'first_discovery_endpoint_ns','discovery_received_at_ns','discovery_timing_basis','discovery_provenance_hash',
        'post_discovery_eligible','session_segment',*[f+s for f in FEATURES for s in ('','_reason_mask','_analysis_valid','_eda_eligible')]]))


def extension_rows(base_path, quotes, day, symbol, *, batch_size=4096, check=lambda:None, full=True, stats=None, limit=None, halts=None):
    movement=Movement()
    age=MidpointAge(V.events(quotes,'quote',day,batch_size=batch_size,stats=stats),V.session_bounds(day)[0],check)
    intervals=iter(halts or ());halt=next(intervals,None)
    for base in rows(base_path,EXTENSION_BASE_COLUMNS,batch_size,check,limit):
        if base['session_date']!=day or base['symbol']!=symbol:raise ValueError('base partition key mismatch')
        if halts is not None:
            right=base['interval_end_ns'];left=right-NS
            while halt is not None and halt[1]<=left:halt=next(intervals,None)
            active=bool(halt is not None and halt[0]<right and halt[1]>left)
            if base['halt_interval_active']!=active:raise ValueError('base rows disagree with frozen effective halt overlay')
        yield {k:base[k] for k in KEYS}|movement.push(base)|age.push(base)
    if full:age.finish()
    if stats is not None:
        stats['quote_events_reduced']=age.events_consumed
        stats['quote_lookahead_events']=int(age.cursor.current is not None)
        stats['quote_last_output_endpoint_ns']=age.previous_end
        stats['full_event_replay']=full


def schema_from_row(row, metadata):
    # Names determine the explicit public type, never sample values.
    return explicit_schema(row.keys(), metadata)


def write_rows(path, iterator, metadata, *, check=lambda:None, columns=None):
    path=Path(path)
    if path.exists():
        raise FileExistsError(path)
    path.parent.mkdir(parents=True,exist_ok=True)
    partial=path.with_suffix('.partial')
    if partial.exists():
        raise FileExistsError('private partial requires explicit discard/restart: '+str(partial))
    buffer=[];writer=None;count=0
    if columns is not None:
        schema=explicit_schema(columns,metadata)
        writer=pq.ParquetWriter(partial,schema,compression="zstd")
    try:
        for row in iterator:
            row={k:None if isinstance(v,float) and not math.isfinite(v) else v for k,v in row.items()}
            if writer is None:
                schema=schema_from_row(row,metadata)
                writer=pq.ParquetWriter(partial,schema,compression='zstd')
            buffer.append(row);count+=1
            if len(buffer)==1024:
                check();writer.write_table(pa.Table.from_pylist(buffer,schema=schema));buffer.clear()
        if writer is None:
            raise ValueError('empty output')
        if buffer:
            check();writer.write_table(pa.Table.from_pylist(buffer,schema=schema))
    finally:
        if writer is not None:writer.close()
    check()
    if count==0:raise ValueError('empty output')
    footer=pq.ParquetFile(partial)
    if footer.metadata.num_rows!=count or footer.schema_arrow!=schema:raise ValueError('output footer mismatch')
    partial.replace(path)
    return {'rows':count,'sha256':V.sha256(path),'bytes':path.stat().st_size,'schema_sha256':schema_hash(schema)}


def joined_rows(base, extension, discovery, batch_size=4096, check=lambda:None, limit=None):
    for a,b in zip_longest(rows(base,BASE_COLUMNS,batch_size,check,limit),rows(extension,batch_size=batch_size,check=check)):
        if a is None or b is None:
            raise ValueError('missing extension or base row')
        yield assemble(a,b,discovery)


def verify(path,day,symbol,expected_rows,check=lambda:None,trace_ends=()):
    from all_feature_month_verify import verify as independent_verify
    result=independent_verify(path,day,symbol,expected_rows,rows,V.session_bounds(day)[0],check,trace_ends)
    result['sha256']=V.sha256(path)
    return result
