/**
 * Horizon v5 progress polling.
 */
const STEPS = [
  { id: 'scan', min: 5 },
  { id: 'fetch', min: 15 },
  { id: 'nlp', min: 38 },
  { id: 'graph', min: 58 },
  { id: 'gap', min: 82 },
  { id: 'final', min: 92 },
];

const SPINNER = `<svg class="w-3 h-3 animate-spin text-accent" fill="none" viewBox="0 0 24 24">
  <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"/>
  <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z"/>
</svg>`;

const CHECK = `<svg class="w-3 h-3 text-lime" fill="none" stroke="currentColor" viewBox="0 0 24 24">
  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 13l4 4L19 7"/>
</svg>`;

function updateSteps(progress) {
  const active = STEPS.filter((step) => progress >= step.min);
  STEPS.forEach((step) => {
    const el = document.getElementById(`step-${step.id}`);
    if (!el) return;
    const dot = el.querySelector('.step-dot');
    const isActive = active.length > 0 && active[active.length - 1].id === step.id;
    const isDone = progress >= step.min && !isActive;
    el.classList.toggle('active', isActive);
    el.classList.toggle('done', isDone);
    if (dot) {
      dot.innerHTML = isActive
        ? SPINNER
        : (isDone
          ? CHECK
          : '<div class="w-1.5 h-1.5 rounded-full bg-muted/40"></div>');
    }
  });
}

function finishAll() {
  STEPS.forEach((step) => {
    const el = document.getElementById(`step-${step.id}`);
    if (!el) return;
    el.classList.remove('active');
    el.classList.add('done');
    const dot = el.querySelector('.step-dot');
    if (dot) dot.innerHTML = CHECK;
  });
}

function setFailureState(message) {
  const bar = document.getElementById('progress-bar');
  const msg = document.getElementById('status-msg');
  if (bar) {
    bar.style.background = '#f43f5e';
    bar.style.width = '100%';
  }
  if (msg) {
    msg.textContent = message || 'Generation failed — redirecting…';
  }
}

function applyJobState(payload, lastProgress) {
  const bar = document.getElementById('progress-bar');
  const msg = document.getElementById('status-msg');
  const progress = Math.max(payload.progress || 0, payload.progress_state?.current_progress || 0);

  if (bar && progress !== lastProgress.value) {
    bar.style.width = `${progress}%`;
    lastProgress.value = progress;
  }
  if (msg && payload.message) {
    msg.textContent = payload.message;
  }
  updateSteps(progress);

  if (payload.status === 'complete') {
    finishAll();
    if (bar) bar.style.width = '100%';
    if (msg) msg.textContent = 'Ready — redirecting…';
    window.setTimeout(() => {
      window.location.href = `/roadmap/${payload.job_id}`;
    }, 500);
    return true;
  }

  if (payload.status === 'failed') {
    setFailureState(payload.error || payload.message ? `Failed: ${payload.error || payload.message}` : 'Generation failed — redirecting…');
    window.setTimeout(() => {
      window.location.href = `/roadmap/${payload.job_id}`;
    }, 1500);
    return true;
  }

  return false;
}

function startProgress(jobId) {
  const lastProgress = { value: -1 };
  let attempts = 0;
  const poll = async () => {
    attempts += 1;
    try {
      const response = await fetch(`/api/job/${jobId}`, { headers: { Accept: 'application/json' } });
      if (!response.ok) {
        throw new Error(`Status ${response.status}`);
      }
      const payload = await response.json();
      const finished = applyJobState(payload, lastProgress);
      if (!finished && attempts < 180) {
        window.setTimeout(poll, 700);
      }
    } catch (error) {
      const msg = document.getElementById('status-msg');
      if (msg) {
        msg.textContent = 'Reconnecting to roadmap generator…';
      }
      if (attempts < 180) {
        window.setTimeout(poll, 1000);
      } else {
        setFailureState('Unable to reach the roadmap status endpoint.');
      }
    }
  };

  poll();
}
