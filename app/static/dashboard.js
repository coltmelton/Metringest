const byId = (id) => document.getElementById(id);
const number = new Intl.NumberFormat();
const decimal = new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 });
const escapeHtml = (value) => String(value ?? "").replace(/[&<>'"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c]));
const groupBy = (items, key) => items.reduce((groups, item) => {
  const value = key(item); (groups[value] ||= []).push(item); return groups;
}, {});

function chart(container, points, field) {
  if (!points.length) {
    container.innerHTML = '<svg viewBox="0 0 600 220"><text x="300" y="110" text-anchor="middle" class="empty-label">No data in this time range</text></svg>';
    return;
  }
  const width = 600, height = 220, left = 42, right = 10, top = 10, bottom = 25;
  const values = points.map(p => Number(p[field]) || 0);
  const max = Math.max(...values, 1), plotW = width-left-right, plotH = height-top-bottom;
  const coords = values.map((v, i) => [left + (i / Math.max(values.length-1, 1))*plotW, top + plotH-(v/max)*plotH]);
  const line = coords.map((p,i) => `${i?'L':'M'}${p[0].toFixed(1)},${p[1].toFixed(1)}`).join(' ');
  const area = `${line} L${coords.at(-1)[0]},${top+plotH} L${coords[0][0]},${top+plotH} Z`;
  const grids = [0,.25,.5,.75,1].map(r => {
    const y=top+plotH-r*plotH;
    return `<line x1="${left}" y1="${y}" x2="${width-right}" y2="${y}" class="grid-line"/><text x="${left-7}" y="${y+3}" text-anchor="end" class="axis-label">${decimal.format(max*r)}</text>`;
  }).join('');
  const start = new Date(points[0].time).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'});
  const end = new Date(points.at(-1).time).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'});
  container.innerHTML = `<svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="xMidYMid meet">${grids}<path d="${area}" class="area"/><path d="${line}" class="line"/><text x="${left}" y="${height-4}" class="axis-label">${start}</text><text x="${width-right}" y="${height-4}" text-anchor="end" class="axis-label">${end}</text></svg>`;
}

function renderDashboard(data) {
  const totals=data.totals;
  byId('events').textContent=number.format(totals.events);
  byId('rate').textContent=decimal.format(totals.events/(data.range_hours*60));
  byId('metrics').textContent=number.format(totals.metrics);
  byId('sources').textContent=number.format(totals.sources);
  byId('last-event').textContent=totals.last_event ? new Date(totals.last_event).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'}) : 'None';
  byId('throughput-total').textContent=`${number.format(totals.events)} events`;
  chart(byId('throughput-chart'),data.series,'events');
  chart(byId('value-chart'),data.series,'average');
  byId('recent-body').innerHTML=data.recent.length ? data.recent.map(row=>`<tr><td>${new Date(row.occurred_at).toLocaleString()}</td><td>${escapeHtml(row.name)}</td><td>${decimal.format(row.value)}</td><td>${escapeHtml(row.source)}</td><td>${escapeHtml(JSON.stringify(row.tags))}</td></tr>`).join('') : '<tr><td colspan="5" class="empty">No events have been processed yet.</td></tr>';
}

function renderSummaries(rows) {
  byId('summary-body').innerHTML=rows.length ? rows.map(row=>`<tr><td><b>${escapeHtml(row.name)}</b></td><td>${number.format(row.count)}</td><td>${decimal.format(row.last_value)}</td><td>${decimal.format(row.average)}</td><td>${decimal.format(row.minimum)}</td><td>${decimal.format(row.maximum)}</td><td>${new Date(row.updated_at).toLocaleString()}</td></tr>`).join('') : '<tr><td colspan="7" class="empty">No metrics have been processed yet.</td></tr>';
}

function renderHealth(data) {
  byId('services').innerHTML=Object.entries(data.services).map(([name,ok])=>`<li><i class="${ok?'':'error'}"></i>${escapeHtml(name)}<span class="${ok?'':'error'}">${ok?'Operational':'Unavailable'}</span></li>`).join('');
}

const palette=['#2563a6','#3f8f5f','#c17b24','#7857a6','#3d8f99','#b04b59','#718096','#9a7138'];
const emptySvg=message=>`<svg viewBox="0 0 600 280"><text x="300" y="140" text-anchor="middle" class="empty-label">${message}</text></svg>`;
const point=(cx,cy,r,a)=>[cx+Math.cos(a)*r,cy+Math.sin(a)*r];

function radar(events) {
  const target=byId('radar-chart'); if(!events.length){target.innerHTML=emptySvg('No source data');return;}
  const groups=groupBy(events,e=>e.source), names=Object.keys(groups).slice(0,3);
  const allMetrics=new Set(events.map(e=>e.name)).size, maxCount=Math.max(...names.map(n=>groups[n].length));
  const latest=Math.max(...events.map(e=>Date.parse(e.occurred_at))), earliest=Math.min(...events.map(e=>Date.parse(e.occurred_at)));
  const axes=['Vol','Fresh','Continuity','Coverage','Stable'];
  let svg='<svg viewBox="0 0 600 280">';
  names.forEach((name,idx)=>{const cx=100+idx*200,cy=142,r=64,rows=groups[name], values=rows.map(e=>Number(e.value)),mean=values.reduce((a,b)=>a+b,0)/values.length,sd=Math.sqrt(values.reduce((a,b)=>a+(b-mean)**2,0)/values.length), activeBins=new Set(rows.map(e=>Math.floor((Date.parse(e.occurred_at)-earliest)/Math.max((latest-earliest)/12,1)))).size;
    const sourceLatest=Math.max(...rows.map(e=>Date.parse(e.occurred_at)));
    const scores=[rows.length/maxCount,Math.max(0,1-(latest-sourceLatest)/Math.max(latest-earliest,1)),Math.min(activeBins/10,1),new Set(rows.map(e=>e.name)).size/allMetrics,Math.max(0,1-sd/Math.max(Math.abs(mean),.001))];
    for(let ring=1;ring<=4;ring++){const web=axes.map((_,i)=>point(cx,cy,r*ring/4,-Math.PI/2+i*Math.PI*2/5).join(',')).join(' ');svg+=`<polygon points="${web}" class="radar-axis"/>`;}
    axes.forEach((a,i)=>{const p=point(cx,cy,r+12,-Math.PI/2+i*Math.PI*2/5);svg+=`<text x="${p[0]}" y="${p[1]}" text-anchor="middle" class="axis-label">${a}</text>`;});
    const pts=scores.map((v,i)=>point(cx,cy,r*v,-Math.PI/2+i*Math.PI*2/5).join(',')).join(' ');
    svg+=`<polygon points="${pts}" fill="${palette[idx]}22" stroke="${palette[idx]}" stroke-width="2"/>`;
    svg+=`<text x="${cx}" y="248" text-anchor="middle" class="chart-title">${escapeHtml(name)}</text>`;
  }); target.innerHTML=svg+'</svg>';
}

function anomalyTimeline(events) {
  const target=byId('anomaly-chart');if(!events.length){target.innerHTML=emptySvg('No event data');return;}
  const times=events.map(e=>Date.parse(e.occurred_at)),lo=Math.min(...times),hi=Math.max(...times),stats={};
  Object.entries(groupBy(events,e=>e.name)).forEach(([name,items])=>{const values=items.map(e=>+e.value),mean=values.reduce((a,b)=>a+b,0)/values.length,sd=Math.sqrt(values.reduce((a,b)=>a+(b-mean)**2,0)/values.length)||1;stats[name]={mean,sd};});
  const plotted=events.map(e=>({...e,z:Math.max(-4,Math.min(4,(e.value-stats[e.name].mean)/stats[e.name].sd))}));
  let svg='<svg viewBox="0 0 600 280"><rect x="48" y="74" width="514" height="132" fill="#eef6ef"/><rect x="48" y="41" width="514" height="33" fill="#fff4df"/><rect x="48" y="206" width="514" height="33" fill="#fff4df"/>';
  [-4,-2,0,2,4].forEach(z=>{const y=140-z*33;svg+=`<line x1="48" y1="${y}" x2="562" y2="${y}" class="grid-line"/><text x="40" y="${y+3}" text-anchor="end" class="axis-label">${z}σ</text>`;});
  plotted.slice(0,2500).forEach(e=>{const x=48+(Date.parse(e.occurred_at)-lo)/Math.max(hi-lo,1)*514,y=140-e.z*33,abs=Math.abs(e.z),color=abs>=3?'#c43d3d':abs>=2?'#c17b24':'#3f8f5f',radius=abs>=2?2.7:1.5;svg+=`<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="${radius}" fill="${color}" opacity="${abs>=2?.9:.35}"/>`;});
  const anomalies=plotted.filter(e=>Math.abs(e.z)>=2).length;
  svg+=`<text x="48" y="260" class="axis-label">${new Date(lo).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'})}</text><text x="562" y="260" text-anchor="end" class="axis-label">${new Date(hi).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'})}</text><text x="554" y="31" text-anchor="end" class="chart-title">${anomalies} events beyond ±2σ</text></svg>`;target.innerHTML=svg;
}

function arcPath(cx,cy,r0,r1,a0,a1){const p1=point(cx,cy,r1,a0),p2=point(cx,cy,r1,a1),p3=point(cx,cy,r0,a1),p4=point(cx,cy,r0,a0),large=a1-a0>Math.PI?1:0;return `M${p1} A${r1},${r1} 0 ${large} 1 ${p2} L${p3} A${r0},${r0} 0 ${large} 0 ${p4} Z`;}
function sunburst(events){const target=byId('sunburst-chart');if(!events.length){target.innerHTML=emptySvg('No composition data');return;}const sources=groupBy(events,e=>e.source),total=events.length,cx=235,cy=140;let angle=-Math.PI/2,svg='<svg viewBox="0 0 600 280">';Object.entries(sources).forEach(([source,items],si)=>{const end=angle+items.length/total*Math.PI*2;svg+=`<path d="${arcPath(cx,cy,35,70,angle,end)}" fill="${palette[si%palette.length]}" stroke="#fff"/>`;let sub=angle;Object.entries(groupBy(items,e=>e.name)).forEach(([name,metrics],mi)=>{const subEnd=sub+metrics.length/total*Math.PI*2;svg+=`<path d="${arcPath(cx,cy,72,106,sub,subEnd)}" fill="${palette[(si+mi+2)%palette.length]}" stroke="#fff"/>`;let reg=sub;Object.values(groupBy(metrics,e=>e.tags?.region||'unknown')).forEach(region=>{const regEnd=reg+region.length/total*Math.PI*2;svg+=`<path d="${arcPath(cx,cy,108,128,reg,regEnd)}" fill="${palette[(si+mi+4)%palette.length]}" opacity=".72" stroke="#fff"/>`;reg=regEnd;});sub=subEnd;});svg+=`<rect x="390" y="${35+si*20}" width="9" height="9" fill="${palette[si]}"/><text x="405" y="${43+si*20}" class="legend-label">${escapeHtml(source)} (${items.length})</text>`;angle=end;});svg+=`<text x="${cx}" y="${cy}" text-anchor="middle" class="chart-title">${number.format(total)}</text><text x="${cx}" y="${cy+13}" text-anchor="middle" class="axis-label">events</text></svg>`;target.innerHTML=svg;}

function stream(events){const target=byId('stream-chart');if(!events.length){target.innerHTML=emptySvg('No traffic mix data');return;}const grouped=groupBy(events,e=>e.name),names=Object.keys(grouped).sort((a,b)=>grouped[b].length-grouped[a].length).slice(0,6),times=events.map(e=>Date.parse(e.occurred_at)),lo=Math.min(...times),hi=Math.max(...times),bins=30,series=names.map(()=>Array(bins).fill(0));events.forEach(e=>{const n=names.indexOf(e.name);if(n>=0)series[n][Math.min(bins-1,Math.floor((Date.parse(e.occurred_at)-lo)/Math.max(hi-lo,1)*bins))]++;});const totals=Array.from({length:bins},(_,i)=>series.reduce((s,row)=>s+row[i],0)),max=Math.max(...totals,1),w=510/(bins-1),scale=180/max;let lower=totals.map(v=>125-v*scale/2),svg='<svg viewBox="0 0 600 280">';series.forEach((row,idx)=>{const upper=lower.map((v,i)=>v+row[i]*scale),top=upper.map((v,i)=>`${42+i*w},${v}`).join(' '),bottom=lower.map((v,i)=>`${42+(bins-1-i)*w},${lower[bins-1-i]}`).join(' ');const lx=42+(idx%3)*180,ly=240+Math.floor(idx/3)*16;svg+=`<polygon points="${top} ${bottom}" fill="${palette[idx]}" class="stream-layer"/><rect x="${lx}" y="${ly}" width="8" height="8" fill="${palette[idx]}"/><text x="${lx+12}" y="${ly+8}" class="legend-label">${escapeHtml(names[idx])}</text>`;lower=upper;});target.innerHTML=svg+'</svg>';}

function renderAnalytics(data){radar(data.events);anomalyTimeline(data.events);sunburst(data.events);stream(data.events);}

function openChart(panel) {
  const source=panel.querySelector('.chart');
  if(!source) return;
  byId('dialog-title').textContent=panel.querySelector('h2')?.textContent || 'Chart';
  byId('dialog-description').textContent=panel.querySelector('.panel-head p')?.textContent || '';
  const copy=source.cloneNode(true); copy.removeAttribute('id');
  byId('dialog-chart').replaceChildren(copy);
  byId('chart-dialog').showModal();
}

document.querySelectorAll('[data-expandable]').forEach(panel=>{
  panel.querySelector('.expand')?.addEventListener('click',event=>{event.stopPropagation();openChart(panel);});
  panel.querySelector('.chart')?.addEventListener('click',()=>openChart(panel));
  panel.querySelector('.chart')?.setAttribute('title','Click to expand');
});
byId('dialog-close').addEventListener('click',()=>byId('chart-dialog').close());
byId('chart-dialog').addEventListener('click',event=>{
  if(event.target===byId('chart-dialog')) byId('chart-dialog').close();
});

async function load() {
  const connection=byId('connection');
  try {
    const hours=byId('range').value;
    const [dashboard, summaries, health, analytics]=await Promise.all([
      fetch(`/v1/dashboard?hours=${hours}`), fetch('/v1/metrics?limit=50'), fetch('/health'), fetch(`/v1/analytics?hours=${hours}`)
    ]);
    if (![dashboard,summaries,health,analytics].every(r=>r.ok)) throw new Error('Dashboard request failed');
    renderDashboard(await dashboard.json());
    renderSummaries(await summaries.json());
    renderHealth(await health.json());
    renderAnalytics(await analytics.json());
    connection.className='connection'; connection.innerHTML='<i></i> Connected';
    byId('updated').textContent=new Date().toLocaleTimeString();
  } catch (error) {
    connection.className='connection error'; connection.innerHTML='<i></i> Disconnected';
    console.error(error);
  }
}

byId('refresh').addEventListener('click',load);
byId('range').addEventListener('change',load);
load();
setInterval(load,30000);
