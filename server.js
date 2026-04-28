/**
 * pulso-sync — Railway backend para Control de Cuotas Pulso
 * MULTI-SURVEY: cada encuesta tiene su propio timer e interval.
 *
 * Endpoints:
 *  GET  /                       → health check
 *  GET  /salud                  → health check (alias)
 *  GET  /surveys                → lista de encuestas SM
 *  GET  /surveys/:id/questions  → preguntas de la encuesta
 *  GET  /sync/list              → lista de syncs activos
 *  POST /sync/config            → agrega/actualiza el sync de UNA encuesta
 *  POST /sync/now   { surveyId? } → sync inmediato (1 encuesta o todas)
 *  POST /sync/stop  { surveyId? } → detiene polling (1 encuesta o todas)
 */

'use strict';

const express = require('express');
const cors    = require('cors');
const axios   = require('axios');
const { initializeApp } = require('firebase/app');
const { getDatabase, ref, set, get, remove, onValue } = require('firebase/database');

// ──────────────────────────────────────────
// CONFIG
// ──────────────────────────────────────────
const SM_TOKEN   = process.env.SM_TOKEN;
const FB_API_KEY = process.env.FB_API_KEY;
const FB_DB_URL  = process.env.FB_DB_URL || 'https://control-cuotas-pulso-default-rtdb.firebaseio.com';
const PORT       = process.env.PORT || 3000;

// ──────────────────────────────────────────
// FIREBASE INIT
// ──────────────────────────────────────────
let db, fbReady = false;
try {
  const fbApp = initializeApp({ apiKey: FB_API_KEY, databaseURL: FB_DB_URL });
  db = getDatabase(fbApp);
  fbReady = true;
  console.log('[firebase] Conectado a', FB_DB_URL);
} catch (e) {
  console.error('[firebase] Error al inicializar:', e.message);
}

// ──────────────────────────────────────────
// EXPRESS
// ──────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());

// ──────────────────────────────────────────
// STATE — multi-survey (se pierde si Railway reinicia, restaurado por autoRecover)
// ──────────────────────────────────────────
const syncConfigs   = new Map(); // surveyId(string) → { surveyId, colMap, muestraId, intervalMinutes }
const syncTimers    = new Map(); // surveyId(string) → setInterval handle
const lastSyncTimes = new Map(); // surveyId(string) → ISO string
const choiceCache   = {};         // surveyId(string) → { questionId → { choiceId → text } }
let configListenerSet = false;

// Mutex serial: solo corre UN runSync a la vez (evita rate-limit SM)
let _syncChain = Promise.resolve();
function withSyncLock(fn) {
  const next = _syncChain.then(() => fn().catch(e => console.error('[sync lock]', e?.message || e)));
  _syncChain = next.catch(() => {}); // no romper la cadena
  return next;
}

// ──────────────────────────────────────────
// SM API HELPER
// ──────────────────────────────────────────
async function smGet(path, params = {}) {
  const res = await axios.get(`https://api.surveymonkey.com/v3${path}`, {
    headers: { Authorization: `Bearer ${SM_TOKEN}`, Accept: 'application/json' },
    params,
    timeout: 30000,
  });
  return res.data;
}

// ──────────────────────────────────────────
// UTILS
// ──────────────────────────────────────────
function norm(s) {
  return (s || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim();
}

const PROV_ALIASES = {
  'caba':'caba','ciudad autonoma de buenos aires':'caba','ciudad de buenos aires':'caba',
  'capital federal':'caba','buenos aires ciudad':'caba','c.a.b.a.':'caba',
  'buenos aires':'buenos aires','buenos aires provincia':'buenos aires','provincia de buenos aires':'buenos aires',
  'stgo del estero':'stgo del estero','santiago del estero':'stgo del estero',
  'tierra del fuego':'tierra del fuego',
  'tierra del fuego, antartida e islas del atlantico sur':'tierra del fuego',
  'entre rios':'entre rios','entre ríos':'entre rios',
  'neuquen':'neuquen','neuquén':'neuquen',
  'cordoba':'cordoba','córdoba':'cordoba',
  'tucuman':'tucuman','tucumán':'tucuman',
  'rio negro':'rio negro','río negro':'rio negro',
};
function normProv(s) { const n = norm(s); return PROV_ALIASES[n] || n; }

function normalizeGen(s) {
  const n = norm(s);
  if (n.includes('fem') || n === 'f' || n === 'mujer') return 'Femenino';
  if (n.includes('masc') || n === 'm' || n === 'hombre') return 'Masculino';
  return s;
}

// ──────────────────────────────────────────
// CHOICE LOOKUP
// ──────────────────────────────────────────
async function buildChoiceCache(surveyId) {
  surveyId = String(surveyId);
  if (choiceCache[surveyId]) return choiceCache[surveyId];
  try {
    const data = await smGet(`/surveys/${surveyId}/details`);
    const map = {};
    (data.pages || []).forEach(page => {
      (page.questions || []).forEach(q => {
        const qid = q.id;
        map[qid] = {};
        (q.answers?.choices || []).forEach(c => { map[qid][c.id] = c.text; });
        (q.answers?.rows || []).forEach(r => { map[qid][r.id] = r.text; });
        if (q.answers?.other) map[qid][q.answers.other.id] = q.answers.other.text || 'Otro';
      });
    });
    choiceCache[surveyId] = map;
    console.log(`[choice-cache] Survey ${surveyId}: ${Object.keys(map).length} preguntas cacheadas`);
    return map;
  } catch (e) {
    console.error(`[choice-cache ${surveyId}] Error:`, e.message);
    return {};
  }
}

function resolveAnswer(answer, choiceMap) {
  if (!answer) return null;
  if (answer.text) return answer.text;
  if (answer.choice_id) return choiceMap[answer.choice_id] || answer.choice_id;
  return null;
}

// ──────────────────────────────────────────
// CORE SYNC — SM → Firebase para UNA encuesta
// ──────────────────────────────────────────
async function runSync(surveyId) {
  surveyId = String(surveyId);
  return withSyncLock(async () => {
    const cfg = syncConfigs.get(surveyId);
    if (!cfg) { console.warn(`[sync ${surveyId}] sin config — saltado`); return; }
    const { colMap } = cfg;

    console.log(`[sync ${surveyId}] Iniciando...`);

    const choiceMap = await buildChoiceCache(surveyId);

    const rawCases = [];
    let page = 1;
    let totalResponses = 0;
    let excluded = 0;

    while (true) {
      let data;
      try {
        data = await smGet(`/surveys/${surveyId}/responses/bulk`, { per_page: 100, page });
      } catch (e) {
        console.error(`[sync ${surveyId}] Error en página ${page}:`, e.message);
        break;
      }

      const responses = data.data || [];
      totalResponses = data.total || totalResponses;

      for (const resp of responses) {
        const qMap = {};
        for (const pg of (resp.pages || [])) {
          for (const q of (pg.questions || [])) {
            const qid = q.id;
            if (!qid) continue;
            const qChoices = choiceMap[qid] || {};
            for (const ans of (q.answers || [])) {
              const val = resolveAnswer(ans, qChoices);
              if (val && !qMap[qid]) qMap[qid] = val;
            }
          }
        }

        // Filtro de elegibilidad
        if (colMap.filterCol && colMap.filterVal) {
          const filterAns = qMap[colMap.filterCol];
          if (!filterAns || norm(filterAns) === norm(colMap.filterVal)) { excluded++; continue; }
        }

        const gen   = qMap[colMap.gen]   || '';
        const edad  = qMap[colMap.edad]  || '';
        const prov  = qMap[colMap.prov]  || '';

        let depto = '';
        if (colMap.depto && prov) {
          const np = normProv(prov);
          for (const [provKey, qid] of Object.entries(colMap.depto)) {
            if (normProv(provKey) === np || normProv(provKey).includes(np) || np.includes(normProv(provKey))) {
              depto = qMap[qid] || '';
              break;
            }
          }
        }

        if (!gen || !edad) { excluded++; continue; }

        const caseId = (colMap.idCol && qMap[colMap.idCol])
          ? String(qMap[colMap.idCol])
          : String(resp.id || resp.respondent_id || `R${rawCases.length + 1}`);

        rawCases.push({
          id: caseId, gen: normalizeGen(gen),
          edad: String(parseInt(edad) || edad),
          prov, depto,
        });
      }

      if (responses.length < 100) break;
      page++;
      await new Promise(r => setTimeout(r, 300)); // backoff entre páginas
    }

    const payload = {
      surveyId,
      rawCases,
      lastSync: new Date().toISOString(),
      syncStats: { valid: rawCases.length, excluded, total: totalResponses },
    };

    if (fbReady) {
      try {
        await set(ref(db, `pulso/v4sync/${surveyId}`), JSON.stringify(payload));
        lastSyncTimes.set(surveyId, payload.lastSync);
        console.log(`[sync ${surveyId}] ✓ ${rawCases.length} casos → Firebase (${excluded} excluidos)`);
      } catch (e) {
        console.error(`[sync ${surveyId}] Error escribiendo Firebase:`, e.message);
        throw e;
      }
    }
  });
}

// ──────────────────────────────────────────
// TIMER MANAGEMENT
// ──────────────────────────────────────────
function startSyncTimer(surveyId) {
  surveyId = String(surveyId);
  const cfg = syncConfigs.get(surveyId);
  if (!cfg) return;
  const existing = syncTimers.get(surveyId);
  if (existing) clearInterval(existing);
  const ms = cfg.intervalMinutes * 60 * 1000;
  const t = setInterval(() => {
    runSync(surveyId).catch(e => console.error(`[timer ${surveyId}]`, e.message));
  }, ms);
  syncTimers.set(surveyId, t);
}

function stopSyncTimer(surveyId) {
  surveyId = String(surveyId);
  const t = syncTimers.get(surveyId);
  if (t) clearInterval(t);
  syncTimers.delete(surveyId);
  syncConfigs.delete(surveyId);
}

function stopAllSyncs() {
  for (const [, t] of syncTimers) clearInterval(t);
  syncTimers.clear();
  syncConfigs.clear();
}

// ──────────────────────────────────────────
// AUTO-RECOVERY: al arrancar, restaurar todos los syncs activos desde pulso/v4config
// ──────────────────────────────────────────
async function autoRecoverSyncConfigs() {
  if (!fbReady) return;
  try {
    const snap = await get(ref(db, 'pulso/v4config'));
    const raw = snap.val();
    if (!raw) { console.log('[auto-recover] Sin config en Firebase'); return; }
    const cfg = typeof raw === 'string' ? JSON.parse(raw) : raw;
    const surveys = Array.isArray(cfg.activeSurveys) ? cfg.activeSurveys : [];

    let restored = 0;
    for (const sv of surveys) {
      if (!sv || !sv.smSurveyId) continue;
      if (!sv.syncActive) continue; // solo restaurar las que estaban activas
      if (!sv.colMap || !sv.colMap.gen || !sv.colMap.edad || !sv.colMap.prov) continue;

      const surveyId = String(sv.smSurveyId);
      const interval = Math.max(5, parseInt(sv.intervalMinutes) || 15);
      syncConfigs.set(surveyId, {
        surveyId,
        colMap: sv.colMap,
        muestraId: sv.muestraId,
        intervalMinutes: interval,
      });
      startSyncTimer(surveyId);
      restored++;
      console.log(`[auto-recover] ✓ Survey ${surveyId} (${sv.smTitle || ''}) cada ${interval} min`);
      // Sync inmediato (background; el mutex serializa)
      runSync(surveyId).catch(e => console.error(`[auto-recover ${surveyId}]`, e.message));
    }
    console.log(`[auto-recover] Restauradas ${restored} encuesta(s)`);
  } catch (e) {
    console.error('[auto-recover] Error:', e.message);
  }
}

// ──────────────────────────────────────────
// CONFIG LISTENER
// Detecta cambios en pulso/v4config:
//  - Encuestas removidas del dashboard → detiene timer + limpia pulso/v4sync/{id}
//  - syncActive flipped to false → detiene timer (sin borrar datos)
// (Encuestas nuevas / re-activadas se manejan via /sync/config explícito desde la UI)
// ──────────────────────────────────────────
function startConfigListener() {
  if (!fbReady || configListenerSet) return;
  configListenerSet = true;
  onValue(ref(db, 'pulso/v4config'), async (snap) => {
    try {
      const raw = snap.val();
      if (!raw) return;
      const cfg = typeof raw === 'string' ? JSON.parse(raw) : raw;
      const dashboardSurveys = Array.isArray(cfg.activeSurveys) ? cfg.activeSurveys : [];
      const dashboardIds = new Set(dashboardSurveys.map(s => String(s.smSurveyId)).filter(Boolean));
      const dashboardActiveIds = new Set(
        dashboardSurveys.filter(s => s.syncActive && s.smSurveyId).map(s => String(s.smSurveyId))
      );

      // 1) Server tiene corriendo pero ya no está en el dashboard → eliminar
      for (const surveyId of [...syncConfigs.keys()]) {
        if (!dashboardIds.has(surveyId)) {
          console.log(`[listener] Survey ${surveyId} eliminada del dashboard → stop + cleanup`);
          stopSyncTimer(surveyId);
          try { await remove(ref(db, `pulso/v4sync/${surveyId}`)); } catch(_) {}
        }
      }

      // 2) Server tiene corriendo pero syncActive=false → solo stop (sin borrar datos)
      for (const surveyId of [...syncConfigs.keys()]) {
        if (dashboardIds.has(surveyId) && !dashboardActiveIds.has(surveyId)) {
          console.log(`[listener] Survey ${surveyId} pausada → stop timer`);
          stopSyncTimer(surveyId);
        }
      }
    } catch (e) {
      console.error('[listener] Error:', e.message);
    }
  });
  console.log('[listener] Config listener activo');
}

// ──────────────────────────────────────────
// ROUTES — health
// ──────────────────────────────────────────
function healthPayload() {
  const surveys = [];
  for (const [sid, cfg] of syncConfigs) {
    surveys.push({
      surveyId: sid,
      intervalMinutes: cfg.intervalMinutes,
      lastSync: lastSyncTimes.get(sid) || null,
      timerActive: syncTimers.has(sid),
    });
  }
  return {
    status: 'ok',
    firebase: fbReady,
    sm_token: !!SM_TOKEN,
    syncCount: syncConfigs.size,
    surveys,
  };
}
app.get('/',      (_, res) => res.json(healthPayload()));
app.get('/salud', (_, res) => res.json(healthPayload()));

// ──────────────────────────────────────────
// ROUTES — SurveyMonkey passthrough
// ──────────────────────────────────────────
app.get('/surveys', async (_, res) => {
  if (!SM_TOKEN) return res.json({ error: 'SM_TOKEN no configurado' });
  try {
    const data = await smGet('/surveys', { per_page: 50, include: 'response_count' });
    res.json({
      surveys: (data.data || []).map(s => ({
        id: s.id, title: s.title, response_count: s.response_count || 0,
      })),
    });
  } catch (e) {
    console.error('[/surveys]', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.get('/surveys/:id/questions', async (req, res) => {
  if (!SM_TOKEN) return res.json({ error: 'SM_TOKEN no configurado' });
  try {
    const data = await smGet(`/surveys/${req.params.id}/details`);
    const questions = [];
    (data.pages || []).forEach(page => {
      (page.questions || []).forEach(q => {
        questions.push({ id: q.id, heading: q.headings?.[0]?.heading || q.id });
      });
    });
    await buildChoiceCache(req.params.id);
    res.json({ questions });
  } catch (e) {
    console.error('[/questions]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ──────────────────────────────────────────
// ROUTES — sync control
// ──────────────────────────────────────────

// POST /sync/config — agrega o actualiza el sync de UNA encuesta (sin tocar las otras)
app.post('/sync/config', async (req, res) => {
  const { surveyId, colMap, muestraId, intervalMinutes } = req.body || {};

  if (!surveyId || !colMap)        return res.json({ ok: false, error: 'Faltan surveyId o colMap' });
  if (!colMap.gen || !colMap.edad || !colMap.prov)
                                    return res.json({ ok: false, error: 'colMap debe incluir gen, edad y prov' });

  const sid = String(surveyId);
  const interval = Math.max(5, parseInt(intervalMinutes) || 15);

  syncConfigs.set(sid, { surveyId: sid, colMap, muestraId, intervalMinutes: interval });

  // Precachear choices (no bloqueante)
  buildChoiceCache(sid).catch(() => {});

  // (Re)arrancar timer
  startSyncTimer(sid);

  // Sync inmediato en background
  runSync(sid).catch(e => console.error(`[/sync/config → runSync ${sid}]`, e.message));

  console.log(`[/sync/config] Survey ${sid}, cada ${interval} min — total activos: ${syncConfigs.size}`);
  res.json({
    ok: true,
    message: `Sync configurado · cada ${interval} min`,
    activeCount: syncConfigs.size,
  });
});

// POST /sync/now — sync inmediato (1 encuesta o todas)
app.post('/sync/now', (req, res) => {
  const { surveyId } = req.body || {};
  if (surveyId) {
    const sid = String(surveyId);
    if (!syncConfigs.has(sid)) return res.json({ ok: false, error: `Survey ${sid} sin config` });
    res.json({ ok: true, message: `Sync iniciado para ${sid}` });
    runSync(sid).catch(e => console.error(`[/sync/now ${sid}]`, e.message));
  } else {
    if (syncConfigs.size === 0) return res.json({ ok: false, error: 'Sin configuraciones activas' });
    const ids = [...syncConfigs.keys()];
    res.json({ ok: true, message: `Sync iniciado para ${ids.length} encuesta(s)`, surveyIds: ids });
    for (const sid of ids) {
      runSync(sid).catch(e => console.error(`[/sync/now ${sid}]`, e.message));
    }
  }
});

// POST /sync/stop — detiene polling (1 o todas)
app.post('/sync/stop', (req, res) => {
  const { surveyId } = req.body || {};
  if (surveyId) {
    const sid = String(surveyId);
    stopSyncTimer(sid);
    console.log(`[/sync/stop] Survey ${sid} detenida — total activos: ${syncConfigs.size}`);
    res.json({ ok: true, surveyId: sid, activeCount: syncConfigs.size });
  } else {
    const count = syncConfigs.size;
    stopAllSyncs();
    console.log(`[/sync/stop] Todas las syncs detenidas (${count})`);
    res.json({ ok: true, stopped: count });
  }
});

// GET /sync/list — lista de syncs activos
app.get('/sync/list', (_, res) => {
  const surveys = [];
  for (const [sid, cfg] of syncConfigs) {
    surveys.push({
      surveyId: sid,
      intervalMinutes: cfg.intervalMinutes,
      muestraId: cfg.muestraId,
      lastSync: lastSyncTimes.get(sid) || null,
      timerActive: syncTimers.has(sid),
    });
  }
  res.json({ count: surveys.length, surveys });
});

// ──────────────────────────────────────────
// START
// ──────────────────────────────────────────
app.listen(PORT, async () => {
  console.log(`\n  Pulso Sync Server — puerto ${PORT}`);
  console.log(`  SM Token:  ${SM_TOKEN ? '✓ configurado' : '✗ FALTA SM_TOKEN'}`);
  console.log(`  Firebase:  ${fbReady ? '✓ conectado' : '✗ no disponible'}\n`);
  if (fbReady) {
    await autoRecoverSyncConfigs();
    startConfigListener();
  }
});

// Graceful shutdown
process.on('SIGTERM', () => { console.log('SIGTERM — stopAllSyncs'); stopAllSyncs(); process.exit(0); });
process.on('SIGINT',  () => { console.log('SIGINT — stopAllSyncs');  stopAllSyncs(); process.exit(0); });
