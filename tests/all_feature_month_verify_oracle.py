"""Frozen September 9 pre-optimization verifier, used only as a test oracle.

Final-file-only independent reductions and population-correct summaries.

One projected pass, fixed 300-row reference windows, six endpoints, histograms
and a bounded trace set. No production movement/quote accumulator is imported.
Reference quantiles/participation use sorted/max-scaled tiny bounded windows.
"""
from collections import Counter, deque
import bisect
import json
import math
import pyarrow.parquet as pq
from tape_data_product.features.all_feature_month_schema import FEATURES, HORIZONS, REGISTRY, REASONS, VERSION, schema

NS=10**9

def close(a,b,label):
    if a is None and b is None:return
    if a is None or b is None or not math.isclose(a,b,rel_tol=1e-10,abs_tol=1e-12):
        raise ValueError(f'{label} final reconstruction mismatch: {a!r} != {b!r}')

def mean(values):
    return math.fsum(values)/len(values) if values else None

def quantile(values):
    if not values:return None
    values=sorted(values);x=.9*(len(values)-1);i=int(x);a=x-i
    return values[i]*(1-a)+values[min(i+1,len(values)-1)]*a

def participation(values):
    scale=max(values,default=0)
    if not scale:return None
    values=[x/scale for x in values]
    return (math.fsum(values)/math.sqrt(math.fsum(x*x for x in values)))**2/len(values)

def axis(feature):
    if feature.startswith('movement_participation_'):
        return dict(scale='linear',edges=[i/200 for i in range(201)],bins=200,
                    population='field EDA mask',weighting='one second per row',approximation='fixed-bin approximate CDF')
    return dict(scale='log10 with explicit zero and tails',edges=[10**(-6+i/4) for i in range(73)],bins=75,
                population='field EDA mask',weighting='one second per row',approximation='fixed-bin approximate CDF',
                bin_layout='zero; positive underflow; 72 log intervals; overflow >=1e12')

class Summary:
    def __init__(self):self.groups={}
    def push(self,row):
        for f in FEATURES:
            key=(row['session_segment'],f)
            if key not in self.groups:
                ax=axis(f)
                self.groups[key]=dict(rows=0,valid=0,nulls=0,zeros=0,post=0,eda=0,support=0,reason=Counter(),
                    histogram=[0]*ax['bins'],axis=ax,minimum=None,maximum=None)
            g=self.groups[key];value=row[f];reason=row[f+'_reason_mask']
            g['rows']+=1;g['valid']+=row[f+'_analysis_valid'];g['nulls']+=value is None;g['zeros']+=value==0
            g['post']+=row['post_discovery_eligible'];g['eda']+=row[f+'_eda_eligible']
            g['support']+=not bool(reason&REASONS['insufficient_support'])
            for name,bit in REASONS.items():g['reason'][name]+=bool(reason&bit)
            if not row[f+'_eda_eligible']:continue
            if value is None:raise ValueError('eligible null histogram value')
            g['minimum']=value if g['minimum'] is None else min(value,g['minimum'])
            g['maximum']=value if g['maximum'] is None else max(value,g['maximum'])
            if f.startswith('movement_participation_'):
                if not 0<=value<=1:raise ValueError('participation histogram bounds')
                index=min(199,bisect.bisect_right(g['axis']['edges'],value)-1)
            else:
                if value<0:raise ValueError('negative histogram value')
                index=0 if value==0 else 1 if value<1e-6 else 74 if value>=1e12 else 1+bisect.bisect_right(g['axis']['edges'],value)
            g['histogram'][index]+=1
    def result(self):
        result=[]
        for (session,f),g in sorted(self.groups.items()):
            if sum(g['histogram'])!=g['eda']:raise ValueError('histogram eligible mass mismatch')
            result.append(dict(session_segment=session,feature=f,**g))
        return result

class ReferenceSum:
    """Exact rolling sum of persisted binary64 values with exact eviction."""
    def __init__(self,capacity):
        self.capacity=capacity;self.queue=deque();self.n=0;self.units=0
    @staticmethod
    def integer_units(x):
        numerator,denominator=float(x).as_integer_ratio()
        return numerator << (1074-(denominator.bit_length()-1))
    def append(self,x):
        if len(self.queue)==self.capacity:
            old=self.queue.popleft()
            if old is not None:self.units-=old;self.n-=1
        value=None if x is None else self.integer_units(x)
        self.queue.append(value)
        if value is not None:self.units+=value;self.n+=1
    @property
    def total(self):return self.units/(1<<1074)
    def mean(self):return self.units/(self.n*(1<<1074)) if self.n else None


class Reference:
    def __init__(self):
        self.previous=None;self.epoch=None;self.was_halt=False;self.generation=0;self.post=None
        self.reset()
    def reset(self):
        self.ends=deque(maxlen=6);self.history={h:deque(maxlen=h) for h in HORIZONS}
        self.moves={h:deque(maxlen=h-4) for h in HORIZONS}
        self.spreads={h:deque(maxlen=h) for h in HORIZONS}
        self.ages={h:deque(maxlen=h) for h in HORIZONS};self.age_elapsed=0;self.post=None
        self.totals={h:{k:ReferenceSum(h-4 if k=="movement" else h) for k in ("movement","one","trade","dollar","spread","duration","accepted")} for h in HORIZONS}
    def push(self,r):
        t=r['interval_end_ns'];active=r['halt_interval_active'];epoch=r['continuity_segment_id']
        gap=self.previous is not None and (t!=self.previous+NS or epoch!=self.epoch)
        if gap:self.reset()
        resume=self.was_halt and not active
        if resume:self.generation+=1;self.post=0
        if r['halt_resume_boundary']!=resume or r['halt_generation']!=self.generation:raise ValueError('halt generation reconstruction mismatch')
        if active or self.was_halt:
            self.ends.clear();self.age_elapsed=0
            for h in HORIZONS:
                self.ages[h].clear();self.spreads[h].clear()
                for k in ('spread','duration','accepted'):self.totals[h][k]=ReferenceSum(h)
        for key in ('midpoint_valid_duration_ns','quoted_spread_valid_duration_ns'):
            if not isinstance(r[key],int) or not 0<=r[key]<=NS:raise ValueError('invalid integer support duration')
        if r['quoted_spread_valid_duration_ns']>r['midpoint_valid_duration_ns']:raise ValueError('spread exceeds midpoint support')
        if (r['midpoint'] is None)!=(r['midpoint_valid_duration_ns']==0):raise ValueError('midpoint duration support mismatch')
        if r['quoted_spread_integral_bps_seconds']<0 or (r['quoted_spread_valid_duration_ns']==0 and r['quoted_spread_integral_bps_seconds']!=0):raise ValueError('spread integral support mismatch')
        if r['trade_count_1s']<0 or r['dollar_volume_1s']<0:raise ValueError('negative transaction bin')
        qa=r['primitive_quote_source_file_accepted'];ta=r['primitive_trade_source_file_accepted']
        for kind,accepted in [('quote',qa),('trade',ta)]:
            value=r[f'{kind}_age_end_seconds']
            if value is not None and (value<0 or active or not accepted):raise ValueError('invalid current age')
        status=r['midpoint_age_observation_status'];origin=r['midpoint_observation_start_ns']
        age=r['midpoint_change_age_end_seconds'];bound=r['midpoint_no_change_observed_seconds']
        if status not in ('known','no_change_observed','unobservable'):raise ValueError('unknown midpoint age status')
        if status=='known':
            if age is None or age<0 or origin is None or origin>t or age>(t-origin)/NS or bound is not None:raise ValueError('known age/status mismatch')
        elif status=='no_change_observed':
            if age is not None or origin is None or origin>t:raise ValueError('no-change status mismatch')
            close(bound,(t-origin)/NS,'no-change bound')
        elif any(x is not None for x in (origin,age,bound)):raise ValueError('unobservable status mismatch')
        if (active or not qa) and status!='unobservable':raise ValueError('unaccepted observable age')
        expected_d=None;one=False
        if not active:
            self.ends.append((r['midpoint'],qa))
            def change(lag):
                if len(self.ends)<=lag:return None
                history=list(self.ends)[-lag-1:];a,b=history[0][0],history[-1][0]
                if not all(x[1] for x in history) or a is None or b is None or min(a,b)<=0:return None
                return abs(10000*math.log(b/a))
            expected_d=change(5);one=change(1) is not None
        close(r['movement_5s_bps'],expected_d,'retained movement')
        if r['movement_5s_valid']!=(expected_d is not None) or r['movement_1s_valid']!=one:raise ValueError('movement primitive support mismatch')
        if not active:
            self.age_elapsed+=1
            if self.post is not None:self.post+=1
        for h in HORIZONS:
            history=self.history[h];moves=self.moves[h];spreads=self.spreads[h];ages=self.ages[h];sums=self.totals[h]
            if not active:
                history.append((one,ta,r['trade_count_1s'],r['dollar_volume_1s'],r['trade_age_end_seconds'],r['quote_age_end_seconds']))
                moves.append(expected_d);spreads.append((qa,r['quoted_spread_integral_bps_seconds'],r['quoted_spread_valid_duration_ns']))
                ages.append(age)
                for k,x in dict(movement=expected_d,one=int(one),trade=r['trade_count_1s'] if ta else None,
                    dollar=r['dollar_volume_1s'] if ta else None,spread=r['quoted_spread_integral_bps_seconds'] if qa else 0,
                    duration=r['quoted_spread_valid_duration_ns'] if qa else 0,accepted=int(qa)).items():sums[k].append(x)
            d=[x for x in moves if x is not None];n=len(d);count=len(history)
            movement_support=count>=math.ceil(.8*h) and n>=math.ceil(.8*(h-4)) and int(sums['one'].total)>=math.ceil(.8*h)
            activity_count=sums['trade'].n
            trades=[x[4] for x in history if x[4] is not None];quotes=[x[5] for x in history if x[5] is not None]
            exact=[x for x in ages if x is not None]
            structural=len(spreads)==h and sums['accepted'].total==h
            duration=int(sums['duration'].total)
            spread=sums['spread'].total/(duration/NS) if structural and duration else None
            fully=not active and (self.post is None or (count==h and self.post>=h+1))
            diagnostics=dict(state_observed_second_count=count,state_mature=count==h,movement_valid_5s_count=n,
                movement_valid_1s_count=int(sums['one'].total),movement_support_valid=movement_support,
                activity_valid_second_count=activity_count,activity_support_valid=activity_count>=math.ceil(.8*h),
                trade_age_observation_count=len(trades),quote_age_observation_count=len(quotes),
                quoted_spread_valid=structural and duration*10>=9*h*NS and not active,
                state_fully_post_halt=fully,midpoint_change_age_observation_count=len(exact),
                midpoint_change_age_mature=self.age_elapsed>=h and not active,
                midpoint_change_age_support_valid=len(exact)>=math.ceil(.8*h),movement_zero_total=n>0 and not any(d))
            for k,v in diagnostics.items():
                if r[f'{k}_{h}s']!=v:raise ValueError(f'{k} gate reconstruction mismatch')
            support=dict(movement=movement_support,activity=diagnostics['activity_support_valid'],spread=diagnostics['quoted_spread_valid'],
                trade_age=len(trades)>=math.ceil(.8*h),quote_age=len(quotes)>=math.ceil(.8*h),mid_age=len(exact)>=math.ceil(.8*h))
            m=sums['movement'].mean()
            expected=dict(movement_mean_5s_bps=m,movement_participation=participation(d) if count==h and movement_support else None,
                quoted_spread_mean_bps=spread,trade_rate=sums['trade'].mean(),dollar_rate=sums['dollar'].mean(),
                trade_age_p90_seconds=quantile(trades),quote_age_p90_seconds=quantile(quotes),
                midpoint_change_age_p90_seconds=quantile(exact) if self.age_elapsed>=h and support['mid_age'] and qa and not active else None,
                movement_mean_to_spread=m/spread if m is not None and spread is not None and spread>0 else None)
            reasons={}
            for name,(_,_,_,family) in REGISTRY.items():
                if family=='ratio':continue
                f=f'{name}_{h}s';value=r[f];accepted=ta if family in ('activity','trade_age') else qa
                mature=diagnostics['midpoint_change_age_mature'] if family=='mid_age' else count==h
                reason=(1 if value is None else 0)|(2 if not accepted else 0)|(4 if not mature else 0)|(8 if not support[family] else 0)|(16 if active else 0)
                if name=='movement_participation' and diagnostics['movement_zero_total']:reason|=128
                reasons[name]=reason
            reason=reasons['movement_mean_5s_bps']|reasons['quoted_spread_mean_bps']
            if r[f'quoted_spread_mean_bps_{h}s'] is None or r[f'quoted_spread_mean_bps_{h}s']<=0:reason|=64
            if r[f'movement_mean_to_spread_{h}s'] is None:reason|=1
            reasons['movement_mean_to_spread']=reason
            for name,reason in reasons.items():
                f=f'{name}_{h}s';valid=reason==0;carried=not fully and name!='midpoint_change_age_p90_seconds'
                if carried:reason|=32
                eligible=valid and not carried and r['post_discovery_eligible']
                if r[f+'_reason_mask']!=reason or r[f+'_analysis_valid']!=valid or r[f+'_eda_eligible']!=eligible:raise ValueError(f'{f} mask/reason mismatch')
                if valid and not carried:close(r[f],expected[name],f)
                if name=='movement_participation' and r[f] is not None:
                    if not n or not 1/n-1e-12<=r[f]<=1+1e-12:raise ValueError('participation bounds')
        self.previous,self.epoch,self.was_halt=t,epoch,active


def verify(path,day,symbol,expected_rows,reader,start,check=lambda:None,trace_ends=()):
    pf=pq.ParquetFile(path);meta=json.loads((pf.schema_arrow.metadata or {}).get(b'eda',b'{}'))
    if meta.get('version')!=VERSION:raise ValueError('old or missing output version')
    from tape_data_product.features.all_feature_month_core import final_columns
    columns=final_columns()
    if set(pf.schema_arrow.names)!=set(columns) or not pf.schema_arrow.remove_metadata().equals(schema(pf.schema_arrow.names,{}).remove_metadata()):raise ValueError('V2 explicit schema membership/type mismatch')
    counts=Counter();summary=Summary();reference=Reference();traces=[];n=0
    if len(trace_ends)>64:raise ValueError('trace selection exceeds fixed bound')
    for r in reader(path,columns=columns,check=check):
        n+=1
        if (r['session_date'],r['symbol'],r['interval_end_ns'])!=(day,symbol,start+n*NS):raise ValueError('noncanonical or duplicate assembled key')
        if any(isinstance(x,float) and not math.isfinite(x) for x in r.values()):raise ValueError('nonfinite persisted value')
        reference.push(r);summary.push(r)
        if r['interval_end_ns'] in trace_ends:traces.append(r)
        for f in FEATURES:
            for label,value in [('rows',1),('valid',r[f+'_analysis_valid']),('null',r[f] is None),('zero',r[f]==0),('post_discovery',r['post_discovery_eligible']),('eda',r[f+'_eda_eligible'])]:counts[f+'|'+label]+=value
    if n!=expected_rows:raise ValueError('wrong assembled row count')
    return dict(rows_verified=n,counts=dict(counts),coverage=summary.result(),traces=traces,
                reconstructed_fields=list(FEATURES),numerical_diagnostic_recomputations=0,numerical_bound_corrections=0)
