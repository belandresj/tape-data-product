"""Direct frozen 60/300-second product, with one decode/integration per stream.

O(T+Q+N F+N H) time and O(B input_width+H F) state, H<=300,
B<=25000 (4096 default). No full V3 FeatureStream or public-wide assembler.
The unchanged market semantic decoder retains every required vendor input.
"""
from collections import Counter, deque
from pathlib import Path
import math
import sys
import time
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'04_research'))
import all_feature_month_core as C
import all_feature_month_schema as REG
import compact_product_schema as S
from all_feature_month_moments import Moments
V = C.V
NS = V.NS
NAN = math.nan


def events(path, stream, day, batch_size=4096, stats=None, check=lambda: None):
    """Full semantic decoding without signature masks or displayed-depth reduction."""
    if not 1 <= batch_size <= 25000: raise ValueError('raw batch size must be 1..25000')
    pf = pq.ParquetFile(path)
    columns = V.MARKET.QUOTE_COLUMNS if stream == 'quote' else V.TRADE_COLUMNS
    if set(columns)-set(pf.schema_arrow.names): raise ValueError('missing required vendor fields')
    start = C.session_start(day); previous = None
    batches=iter(pf.iter_batches(batch_size=batch_size,columns=list(columns),use_threads=False))
    while True:
        decode_started=time.perf_counter()
        try:batch=next(batches)
        except StopIteration:return
        check()
        if stats is not None:stats[stream+'_rows_decoded'] += batch.num_rows
        table = pa.Table.from_batches([batch])
        for key in ('sip_timestamp','sequence_number'):
            if table[key].null_count: raise ValueError('null '+stream+' event key')
        sip = table['sip_timestamp'].to_numpy(); seq = table['sequence_number'].to_numpy()
        if stream == 'quote':
            sem = V.MARKET._quote_batch_semantics(table)
            if np.any(sem['unknown_condition']) or np.any(sem['unknown_indicator']):
                raise ValueError('unaccepted unknown quote condition/indicator')
            names = ('price_state_valid','locked','midpoint','spread_bps')
            values = zip(*(sem[k].tolist() for k in names))
        else:
            sem = V.MARKET.activity_trade_semantics(table)
            if np.any(sem.unknown_trade_condition): raise ValueError('unaccepted unknown trade condition')
            if np.any(~np.isin(sem.correction_code,list(V.MARKET.KNOWN_CORRECTION_CODES))):
                raise ValueError('unaccepted unknown trade correction')
            eligible = np.where((sip >= start+19800*NS)&(sip < start+43200*NS),
                sem.eligible_activity_trade,sem.eligible_extended_hours_activity_trade)
            names = ('eligible','price','shares')
            values = zip(eligible.tolist(),table['price'].to_numpy().tolist(),sem.analytic_size.tolist())
        if stats is not None:stats['decode_seconds']+=time.perf_counter()-decode_started
        for i,(ts,sequence,value) in enumerate(zip(sip.tolist(),seq.tolist(),values)):
            key = (ts,sequence)
            if previous is not None and key <= previous: raise ValueError('duplicate or unordered '+stream+' event key')
            previous = key
            if stats is not None: stats[stream+'_rows_consumed'] += 1
            if i % 1024 == 0: check()
            yield ts,dict(zip(names,value))


class Window:
    def __init__(self,h):
        self.h=h; self.generations=deque(maxlen=h); self.generation_counts=Counter()
        self.moves=Moments(h-4); self.ones=C.SupportCount(h)
        self.trades=V.SumValues(h); self.dollars=V.SumValues(h)
        self.trade_age=V.RollingValues(h); self.quote_age=V.RollingValues(h)
        self.cost={k:V.SumValues(h) for k in ('spread','duration','accepted')}
    def append(self,generation,move,one,count,dollars,trade_age,quote_age,spread,duration):
        if len(self.generations)==self.h:
            old=self.generations[0];self.generation_counts[old]-=1
            if not self.generation_counts[old]:del self.generation_counts[old]
        self.generations.append(generation);self.generation_counts[generation]+=1
        self.moves.append(move);self.ones.append(0. if one else NAN)
        self.trades.append(count);self.dollars.append(dollars)
        self.trade_age.append(trade_age);self.quote_age.append(quote_age)
        for k,v in dict(spread=spread,duration=duration,accepted=1).items():self.cost[k].append(v)


class Reducer:
    def __init__(self):
        self.previous=self.epoch=None;self.was_halt=False;self.generation=0
        self.reset()
    def reset(self):
        self.ends=deque(maxlen=6);self.windows={h:Window(h) for h in REG.HORIZONS}
        self.post=None
    def push(self,context,age,count,dollars,trade_age,quote_age):
        t=context['interval_end_ns'];active=context['halt_interval_active'];epoch=context['continuity_segment_id']
        if self.previous is not None and (epoch!=self.epoch or t!=self.previous+NS): self.reset()
        resume=self.was_halt and not active
        if resume:self.generation+=1;self.post=0
        if active or self.was_halt:
            self.ends.clear()
            for w in self.windows.values():
                for x in w.cost.values():x.clear()
        move=NAN;one=False
        if not active:
            if self.post is not None:self.post+=1
            self.ends.append(context['midpoint'])
            def change(lag):
                if len(self.ends)<=lag:return NAN
                a,b=self.ends[-lag-1],self.ends[-1]
                return abs(10000*math.log(b/a)) if C.finite(a) and C.finite(b) and min(a,b)>0 else NAN
            move=change(5);one=C.finite(change(1))
        row={}
        row.update(context);row.update(age)
        row.update(movement_5s_bps=move,movement_5s_valid=C.finite(move),movement_1s_valid=one,
            halt_resume_boundary=resume,halt_generation=self.generation,trade_count_1s=count,dollar_volume_1s=dollars,
            trade_age_end_seconds=trade_age/1000 if C.finite(trade_age) else None,
            quote_age_end_seconds=quote_age/1000 if C.finite(quote_age) else None,
            seconds_since_halt_resume=self.post if not active else None)
        for h,w in self.windows.items():
            def put(k,v):row[f'{k}_{h}s']=v
            if not active:
                w.append(self.generation,move,one,count,dollars,trade_age,quote_age,
                    age['quoted_spread_integral_bps_seconds'],age['quoted_spread_valid_duration_ns'])
            n=len(w.generations);support=n>=math.ceil(.8*h) and w.moves.count>=math.ceil(.8*(h-4)) and w.ones.count>=math.ceil(.8*h)
            for k,v in dict(state_observed_second_count=n,state_mature=n==h,
                movement_valid_5s_count=w.moves.count,movement_valid_1s_count=w.ones.count,
                movement_support_valid=support,activity_valid_second_count=w.trades.count,
                activity_support_valid=w.trades.count>=math.ceil(.8*h),
                trade_age_observation_count=w.trade_age.count,quote_age_observation_count=w.quote_age.count,
                movement_zero_total=w.moves.count>0 and w.moves.positive==0).items():put(k,v)
            # Legacy means and endpoint ages carry through closed seconds; cost clears.
            for k,v in dict(movement_mean_5s_bps=(6*w.moves.mean())/6,trade_rate=w.trades.mean(),
                dollar_rate=w.dollars.mean(),trade_age_p90_seconds=w.trade_age.quantile(.9)/1000,
                quote_age_p90_seconds=w.quote_age.quantile(.9)/1000,
                movement_participation=w.moves.participation() if n==h and support else NAN).items():put(k,v)
            c=w.cost;duration=c['duration'].total
            structural=len(c['accepted'].queue)==h and c['accepted'].total==h
            spread=c['spread'].total/(duration/NS) if structural and duration>0 and not active else NAN
            put('quoted_spread_mean_bps',spread)
            put('quoted_spread_valid_fraction',duration/(h*NS) if structural and not active else NAN)
            put('quoted_spread_valid',bool(structural and duration*10>=9*h*NS and not active))
            fully=not active and (self.post is None or (n==h and self.post>=h+1))
            pre=n-w.generation_counts[self.generation] if self.post is not None else 0
            put('state_fully_post_halt',fully)
            put('state_pre_halt_observation_fraction',1. if active and n else pre/n if n else 0.)
            put('state_contains_pre_halt_history',bool(n) if active else pre>0)
            put('state_post_halt_observed_seconds',0 if active else min(h,self.post or 0))
            put('state_carried_forward_during_halt',active and n>0)
            for name,(_,_,_,family) in REG.REGISTRY.items():
                if family=='ratio':continue
                f=f'{name}_{h}s';reason=C.quality(row,h,family,row[f])
                if name=='movement_participation' and row[f'movement_zero_total_{h}s']:reason|=128
                row[f+'_reason_mask']=reason
            f=f'movement_mean_to_spread_{h}s';m=row[f'movement_mean_5s_bps_{h}s']
            value=m/spread if C.finite(m) and C.finite(spread) and spread>0 else NAN
            reason=row[f'movement_mean_5s_bps_{h}s_reason_mask']|row[f'quoted_spread_mean_bps_{h}s_reason_mask']
            if not C.finite(spread) or spread<=0:reason|=64
            if not C.finite(value):reason|=1
            row[f]=value;row[f+'_reason_mask']=reason
            for f in C.FEATURES_BY_HORIZON[h]:
                if not fully and not f.startswith('midpoint_change_age'):row[f+'_reason_mask']|=32
        self.previous,self.epoch,self.was_halt=t,epoch,active
        # Only small fixed state is retained; no output-row accumulation.
        return row


def product_pairs(quotes,trades,day,symbol,discovery,*,seconds=57600,halts=(),batch_size=4096,
                  continuity_breaks_ns=(),stats=None,check=lambda:None):
    if not 1<=seconds<=57600:raise ValueError('seconds must be 1..57600')
    if not 1<=batch_size<=25000:raise ValueError('invalid batch size')
    start=C.session_start(day);breaks=set(continuity_breaks_ns);intervals=sorted(halts)
    if any((b-start)%NS for b in breaks):raise ValueError('unaligned continuity break')
    if any(a>=b for a,b,_ in intervals) or any(intervals[i][0]<intervals[i-1][1] for i in range(1,len(intervals))):raise ValueError('invalid or overlapping halts')
    age=C.MidpointAge(events(quotes,'quote',day,batch_size,stats,check),start,check)
    trade=V.Cursor(events(trades,'trade',day,batch_size,stats,check))
    while trade.current and trade.current[0]<start:trade.pop()
    latest_trade=None;epoch=index=0;previous_halt=False;model=Reducer()
    meta=dict(zip(S.DISCOVERY_FIELDS,(discovery.get('endpoint_ns'),discovery.get('received_at_ns'),
        discovery.get('timing_basis','unavailable'),discovery.get('provenance_hash'))))
    for position in range(seconds):
        reduce_started=time.perf_counter();decode_before=stats.get('decode_seconds',0.) if stats is not None else 0.
        left=start+position*NS;right=left+NS
        while index<len(intervals) and intervals[index][1]<=left:index+=1
        interval=intervals[index] if index<len(intervals) else None
        active=bool(interval and interval[0]<right and interval[1]>left)
        reset=left in breaks;resume=previous_halt and not active
        if reset and not active and not previous_halt:epoch+=1
        if reset or active or resume:latest_trade=None
        context=dict(session_date=day,symbol=symbol,interval_end_ns=right,continuity_segment_id=epoch,
            halt_interval_active=active,halt_interval_id=interval[2] if active else None,
            historical_ex_post_overlay=True,live_reproducible=False,
            primitive_quote_source_file_accepted=True,primitive_trade_source_file_accepted=True,
            post_discovery_eligible=bool(discovery.get('verified') and discovery.get('endpoint_ns') is not None and right>=discovery['endpoint_ns']))
        # A continuity break at the very first row also clears warm-up state.
        if reset:age.reset()
        observed=age.push(context)
        context['midpoint']=age.midpoint_integral.mean()
        qage=(right-age.latest_quote)/1e6 if age.latest_quote is not None and age.current and age.current['price_state_valid'] else NAN
        count=0;dollars=0.
        while trade.current and trade.current[0]<right:
            ts,value=trade.pop()
            if not active and value['eligible']:
                count+=1;dollars+=value['price']*value['shares'];latest_trade=ts
        if not math.isfinite(dollars) or dollars<0:raise ValueError('invalid transaction accumulation')
        tage=(right-latest_trade)/1e6 if latest_trade is not None else NAN
        row=model.push(context,observed,count,dollars,tage,qage)
        def select(columns):
            return {k:None if isinstance(row[k],float) and not math.isfinite(row[k]) else row[k] for k in columns}
        features,support=select(S.FEATURE_COLUMNS),select(S.SUPPORT_COLUMNS)
        if stats is not None:
            stats['output_rows']+=1
            stats['reduce_seconds']+=time.perf_counter()-reduce_started-(stats.get('decode_seconds',0.)-decode_before)
        yield features,support,meta
        check();previous_halt=active
    if seconds==57600:
        age.finish()
        while trade.current:trade.pop()
