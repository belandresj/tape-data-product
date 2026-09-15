"""Offline rendering from reconciled endpoint distribution outputs."""
import math
import matplotlib.pyplot as plt
import numpy as np
import pyarrow.parquet as pq
from .endpoint_distributions import DistributionError
def _label(scope):
    try:return {"synthetic":"SYNTHETIC EXAMPLE","pilot":"STRATIFIED DEVELOPMENT PILOT","full_population":"FULL POPULATION"}[scope]
    except KeyError: raise DistributionError("invalid figure scope")
def render_marginal(table,summary,output,*,title,estimator,population,scope,lineage):
    data=[]; previous=None; last_kept=(0.,0.); epsilon=.0001
    for b in pq.ParquetFile(table).iter_batches(batch_size=4096):
        for r in zip(b["value"].to_pylist(),b["pooled_ecdf"].to_pylist(),b["equal_member_ecdf"].to_pylist()):
            if not data or max(r[1]-last_kept[0],r[2]-last_kept[1])>=epsilon:
                if previous is not None and previous!=data[-1]: data.append(previous)
                data.append(r); last_kept=(r[1],r[2])
            previous=r
    if previous is not None and previous!=data[-1]: data.append(previous)
    fig,ax=plt.subplots(figsize=(8,5)); fig.subplots_adjust(bottom=.27)
    if data:
        a=np.asarray(data); ax.step(a[:,0],a[:,1],where="post",label="Pooled observations"); ax.step(a[:,0],a[:,2],where="post",label="Equal symbol-day"); ax.legend()
    else: ax.text(.5,.5,"No valid observations",ha="center",transform=ax.transAxes)
    ax.set(title=title,xlabel=f"{summary['field']} ({summary['unit']})",ylabel="ECDF  P(X ≤ x)",ylim=(0,1.01)); ax.grid(alpha=.2)
    z=summary["zeros"]/summary["valid"] if summary["valid"] else None; note=f"{_label(scope)} · {population} · {estimator}\nselected={summary['selected']:,}; valid={summary['valid']:,}; unavailable={summary['unavailable']:,}; members={summary['contributing_members']}/{summary['selected_members']}; zero mass={z:.2%}" if z is not None else f"{_label(scope)} · valid=0 · unavailable={summary['unavailable']:,}"
    fig.text(.01,.01,note+f"\nLineage: {lineage}",fontsize=8); fig.savefig(output,dpi=170); plt.close(fig)
def render_joint(summary,output,*,title,estimator,population,scope,lineage):
    p=np.asarray(summary["pooled_probability"]); e=np.asarray(summary["equal_member_probability"]); need=max(p.max(initial=0),e.max(initial=0),1e-12); fig,axes=plt.subplots(1,2,figsize=(13,7)); fig.subplots_adjust(left=.08,right=.88,bottom=.29,top=.86,wspace=.34)
    for ax,a,label in zip(axes,(p,e),("Pooled observations","Equal symbol-day")):
        im=ax.imshow(a.T,origin="lower",aspect="auto",vmin=0,vmax=need); ax.set_title(label); ax.set_xlabel(f"{summary['x']['field']} ({summary['x']['unit']})"); ax.set_ylabel(f"{summary['y']['field']} ({summary['y']['unit']})"); ax.set_xticks(range(len(summary['x']['labels'])),summary['x']['labels'],rotation=55,ha="right",fontsize=7); ax.set_yticks(range(len(summary['y']['labels'])),summary['y']['labels'],fontsize=7)
    fig.colorbar(im,ax=axes,label="Probability mass"); fig.suptitle(title); fig.text(.01,.01,f"{_label(scope)} · {population} · {estimator}\npair-valid={summary['pair_valid']:,}/{summary['selected']:,}; members={summary['contributing_members']}/{summary['selected_members']}; x-zero={summary['x_zero_pair_valid']:,}; y-zero={summary['y_zero_pair_valid']:,}; both-zero={summary['zero_zero']:,}\nExplicit zero/underflow/overflow; no winsorization. Lineage: {lineage}",fontsize=8); fig.savefig(output,dpi=170); plt.close(fig)
