/**
 * Horizon v2 — SSE Progress (upgraded)
 */
const STEPS = [
  { id:'scan',  min:5  },
  { id:'fetch', min:15 },
  { id:'nlp',   min:38 },
  { id:'graph', min:58 },
  { id:'gap',   min:82 },
  { id:'final', min:92 },
];

const SPINNER = `<svg class="w-3 h-3 animate-spin text-accent" fill="none" viewBox="0 0 24 24">
  <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"/>
  <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z"/>
</svg>`;

const CHECK = `<svg class="w-3 h-3 text-lime" fill="none" stroke="currentColor" viewBox="0 0 24 24">
  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 13l4 4L19 7"/>
</svg>`;

function updateSteps(progress) {
  const active = STEPS.filter(s => progress >= s.min);
  STEPS.forEach((s, i) => {
    const el = document.getElementById(`step-${s.id}`);
    if (!el) return;
    const dot = el.querySelector('.step-dot');
    const isActive = active.length > 0 && active[active.length - 1].id === s.id;
    const isDone   = progress >= s.min && !isActive;
    el.classList.toggle('active', isActive);
    el.classList.toggle('done', isDone);
    if (dot) dot.innerHTML = isActive ? SPINNER : (isDone ? CHECK : '<div class="w-1.5 h-1.5 rounded-full bg-muted/40"></div>');
  });
}

function finishAll() {
  STEPS.forEach(s => {
    const el = document.getElementById(`step-${s.id}`);
    if (!el) return;
    el.classList.remove('active'); el.classList.add('done');
    const dot = el.querySelector('.step-dot');
    if (dot) dot.innerHTML = CHECK;
  });
}

function startProgress(jobId) {
  const bar = document.getElementById('progress-bar');
  const msg = document.getElementById('status-msg');
  let last = -1;

  const src = new EventSource(`/events/${jobId}`);

  src.onmessage = e => {
    let data;
    try { data = JSON.parse(e.data); } catch { return; }
    const { progress=0, status, message='', done } = data;

    if (bar && progress !== last) { bar.style.width = `${progress}%`; last = progress; }
    if (msg && message) msg.textContent = message;
    updateSteps(progress);

    if (status === 'complete') {
      src.close(); finishAll();
      if (bar) bar.style.width = '100%';
      if (msg) msg.textContent = 'Ready — redirecting…';
      setTimeout(() => window.location.href = `/roadmap/${jobId}`, 500);
    } else if (status === 'failed' || (done && status !== 'complete')) {
      src.close();
      if (bar) { bar.style.background = '#f43f5e'; bar.style.width = '100%'; }
      if (msg) msg.textContent = message ? `Failed: ${message}` : 'Generation failed — redirecting…';
      setTimeout(() => window.location.href = `/roadmap/${jobId}`, 3000);
    }
  };

  src.onerror = () => { src.close(); pollFallback(jobId); };
}

async function pollFallback(jobId) {
  const bar = document.getElementById('progress-bar');
  const msg = document.getElementById('status-msg');
  let attempts = 0;
  const iv = setInterval(async () => {
    if (++attempts > 140) { clearInterval(iv); return; }
    try {
      const d = await (await fetch(`/status/${jobId}`)).json();
      if (bar) bar.style.width = `${d.progress||0}%`;
      if (msg && d.message) msg.textContent = d.message;
      updateSteps(d.progress || 0);
      if (d.status === 'complete') { clearInterval(iv); finishAll(); setTimeout(() => window.location.href=`/roadmap/${jobId}`, 500); }
      if (d.status === 'failed')   { clearInterval(iv); if (bar) { bar.style.background = '#f43f5e'; bar.style.width = '100%'; } if (msg && d.message) msg.textContent = `Failed: ${d.message}`; setTimeout(() => window.location.href=`/roadmap/${jobId}`, 3000); }
    } catch {}
  }, 600);
}
