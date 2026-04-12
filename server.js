/**
 * pulso-sync — Railway backend for Control de Cuotas Pulso
 * Endpoints consumidos por control_cuotas_pulso_v4.html
 *
 * GET  /                         → health check
 * GET  /surveys                  → lista de encuestas SM
 * GET  /surveys/:id/questions    → preguntas de la encuesta
 * POST /sync/config              → configura y arranca polling
 * POST /sync/now                 → sync inmediato
 * POST /sync/stop                → detiene polling
 */

'use strict';

const express = require('express');
const cors    = require('cors');
const axios   = require('axios');
const { initializeApp }   = require('firebase/app');
const { getDatabase, ref, set, get } = require('firebase/database');

// ──────────────────────────────────────────
// CONFIG — variables de entorno en Railway
// ──────────────────────────────────────────
const SM_TOKEN       = process.env.SM_TOKEN;
const FB_API_KEY     = process.env.FB_API_KEY;
const FB_DB_URL      = process.env.FB_DB_URL || 'https://control-cuotas-pulso-default-rtdb.firebaseio.com';
const PORT           = process.env.PORT || 3000;

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
// SYNC CONFIG PERSISTENCE — sobrevive reinicios de Railway
// ──────────────────────────────────────────
const CONFIG_FB_PATH = 'pulso/syncServerConfig';

async function saveSyncConfigToFirebase(cfg) {
  if (!fbReady) return;
  try {
    await set(ref(db, CONFIG_FB_PATH), JSON.stringify(cfg));
    console.log('[config] Guardado en Firebase');
  } catch (e) {
    console.error('[config] Error guardando:', e.message);
  }
}

async function restoreSyncConfigFromFirebase() {
  if (!fbReady) return;
  try {
    const snap = await get(ref(db, CONFIG_FB_PATH));
    const raw = snap.val();
    if (!raw) { console.log('[config] Sin config guardada'); return; }
    const cfg = typeof raw === 'string' ? JSON.parse(raw) : raw;
    if (!cfg.surveyId || !cfg.colMap) return;

    syncConfig = {
      surveyId:        String(cfg.surveyId),
      colMap:          cfg.colMap,
      muestraId:       cfg.muestraId,
      intervalMinutes: Math.max(5, parseInt(cfg.intervalMinutes) || 15),
    };

    // Reiniciar timer
    if (syncTimer) clearInterval(syncTimer);
    syncActive = true;
    const ms = syncConfig.intervalMinutes * 60 * 1000;
    syncTimer = setInterval(() => {
      runSync().catch(e => console.error('[sync timer]', e.message));
    }, ms);

    console.log(`[config] Restaurado desde Firebase: survey ${syncConfig.surveyId}, cada ${syncConfig.intervalMinutes} min`);

    // Sync inmediato al restaurar
    runSync().catch(e => console.error('[config restore → runSync]', e.message));
  } catch (e) {
    console.error('[config] Error restaurando:', e.message);
  }
}

// ──────────────────────────────────────────
// EXPRESS SETUP
// ──────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());

// ──────────────────────────────────────────
// SYNC STATE  (se pierde si Railway reinicia — por diseño)
// ──────────────────────────────────────────
let syncConfig   = null;   // { surveyId, colMap, muestraId, intervalMinutes }
let syncTimer    = null;
let syncActive   = false;
let lastSyncTime = null;
let choiceCache  = {};     // surveyId → { questionId → { choiceId → text } }

// ──────────────────────────────────────────
// SURVEYMONKEY API HELPER
// ──────────────────────────────────────────
async function smGet(path, params = {}) {
  const res = await axios.get(`https://api.surveymonkey.com/v3${path}`, {
    headers: { Authorization: `Bearer ${SM_TOKEN}`, Accept: 'application/json' },
    params,
    timeout: 20000,
  });
  return res.data;
}

// ──────────────────────────────────────────
// UTILS
// ──────────────────────────────────────────
function norm(s) {
  return (s || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim();
}

// Aliases de provincia idénticos al dashboard (para match en depto)
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
// BUILD CHOICE LOOKUP for a survey
// Precarga todos los choices de una encuesta para resolver IDs → texto
// ──────────────────────────────────────────
async function buildChoiceCache(surveyId) {
  if (choiceCache[surveyId]) return choiceCache[surveyId];
  try {
    const data = await smGet(`/surveys/${surveyId}/details`);
    const map = {}; // questionId → { choiceId → text }
    (data.pages || []).forEach(page => {
      (page.questions || []).forEach(q => {
        const qid = q.id;
        map[qid] = {};
        // Single-choice answers
        (q.answers?.choices || []).forEach(c => { map[qid][c.id] = c.text; });
        // Matrix rows
        (q.answers?.rows || []).forEach(r => { map[qid][r.id] = r.text; });
        // Other / open
        if (q.answers?.other) map[qid][q.answers.other.id] = q.answers.other.text || 'Otro';
      });
    });
    choiceCache[surveyId] = map;
    console.log(`[choice-cache] Survey ${surveyId}: ${Object.keys(map).length} preguntas cacheadas`);
    return map;
  } catch (e) {
    console.error('[choice-cache] Error:', e.message);
    return {};
  }
}

// Resuelve el valor de texto de una respuesta dado el mapa de choices
function resolveAnswer(answer, choiceMap) {
  if (!answer) return null;
  // Respuesta abierta / numérica
  if (answer.text) return answer.text;
  // Choice
  if (answer.choice_id) {
    return choiceMap[answer.choice_id] || answer.choice_id;
  }
  return null;
}

// ──────────────────────────────────────────
// CORE SYNC  — SM → Firebase
// ──────────────────────────────────────────
async function runSync() {
  if (!syncConfig) return;
  const { surveyId, colMap } = syncConfig;

  console.log(`[sync] Iniciando sync para encuesta ${surveyId}...`);

  // Aseguramos tener el mapa de choices
  const choiceMap = await buildChoiceCache(surveyId);

  const rawCases = [];
  let page = 1;
  let totalResponses = 0;
  let excluded = 0;

  // Paginar todas las respuestas
  while (true) {
    let data;
    try {
      data = await smGet(`/surveys/${surveyId}/responses/bulk`, {
        per_page: 100,
        page,
      });
    } catch (e) {
      console.error(`[sync] Error en página ${page}:`, e.message);
      break;
    }

    const responses = data.data || [];
    totalResponses = data.total || totalResponses;

    for (const resp of responses) {
      // SM bulk API devuelve resp.pages[].questions[].answers[]
      // (NO un array answers plano en la raíz)
      const qMap = {};
      for (const page of (resp.pages || [])) {
        for (const q of (page.questions || [])) {
          const qid = q.id;
          if (!qid) continue;
          const qChoices = choiceMap[qid] || {};
          for (const ans of (q.answers || [])) {
            const val = resolveAnswer(ans, qChoices);
            if (val && !qMap[qid]) qMap[qid] = val;
          }
        }
      }

      // Aplicar filtro (ej: excluir "not answered" en voto)
      if (colMap.filterCol && colMap.filterVal) {
        const filterAns = qMap[colMap.filterCol];
        // Excluir si no respondió o respondió el valor de exclusión
        if (!filterAns || norm(filterAns) === norm(colMap.filterVal)) {
          excluded++;
          continue;
        }
      }

      // Extraer variables clave
      const gen   = qMap[colMap.gen]   || '';
      const edad  = qMap[colMap.edad]  || '';
      const prov  = qMap[colMap.prov]  || '';

      // Departamento: buscar en el mapa de columnas por provincia
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

      // Saltar si faltan campos críticos
      if (!gen || !edad) { excluded++; continue; }

      // ID del caso: usar idCol si está mapeado, sino respondent_id
      const caseId = (colMap.idCol && qMap[colMap.idCol])
        ? String(qMap[colMap.idCol])
        : String(resp.id || resp.respondent_id || `R${rawCases.length + 1}`);

      rawCases.push({
        id:    caseId,
        gen:   normalizeGen(gen),
        edad:  String(parseInt(edad) || edad),
        prov,
        depto,
      });
    }

    // ¿Hay más páginas?
    if (responses.length < 100) break;
    page++;

    // Pequeña pausa para no agotar rate limit
    await new Promise(r => setTimeout(r, 300));
  }

  // Escribir en Firebase: pulso/v4sync/{surveyId}
  const payload = {
    surveyId: String(surveyId),
    rawCases,
    lastSync: new Date().toISOString(),
    syncStats: {
      valid:    rawCases.length,
      excluded: excluded,
      total:    totalResponses,
    },
  };

  if (fbReady) {
    try {
      const syncRef = ref(db, `pulso/v4sync/${surveyId}`);
      await set(syncRef, JSON.stringify(payload));
      lastSyncTime = payload.lastSync;
      console.log(`[sync] ✓ Survey ${surveyId}: ${rawCases.length} casos → Firebase (${excluded} excluidos)`);
    } catch (e) {
      console.error('[sync] Error escribiendo en Firebase:', e.message);
      throw e;
    }
  } else {
    console.warn('[sync] Firebase no disponible — datos no persistidos');
  }
}

// ──────────────────────────────────────────
// ROUTES
// ──────────────────────────────────────────

// GET / — health check
app.get('/', (req, res) => {
  res.json({
    status:     'ok',
    firebase:   fbReady,
    sm_token:   !!SM_TOKEN,
    syncActive,
    lastSync:   lastSyncTime,
    config:     syncConfig ? {
      surveyId:        syncConfig.surveyId,
      intervalMinutes: syncConfig.intervalMinutes,
    } : null,
  });
});

// GET /surveys — lista encuestas SM
app.get('/surveys', async (req, res) => {
  if (!SM_TOKEN) return res.json({ error: 'SM_TOKEN no configurado' });
  try {
    const data = await smGet('/surveys', { per_page: 50, include: 'response_count' });
    res.json({
      surveys: (data.data || []).map(s => ({
        id:             s.id,
        title:          s.title,
        response_count: s.response_count || 0,
      })),
    });
  } catch (e) {
    console.error('[/surveys]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /surveys/:id/questions — preguntas de la encuesta
app.get('/surveys/:id/questions', async (req, res) => {
  if (!SM_TOKEN) return res.json({ error: 'SM_TOKEN no configurado' });
  try {
    const data = await smGet(`/surveys/${req.params.id}/details`);
    const questions = [];
    (data.pages || []).forEach(page => {
      (page.questions || []).forEach(q => {
        const heading = q.headings?.[0]?.heading || q.id;
        questions.push({ id: q.id, heading });
      });
    });

    // Aprovechar para precachear choices
    await buildChoiceCache(req.params.id);

    res.json({ questions });
  } catch (e) {
    console.error('[/questions]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// POST /sync/config — configura y arranca polling
app.post('/sync/config', async (req, res) => {
  const { surveyId, colMap, muestraId, intervalMinutes } = req.body || {};

  if (!surveyId || !colMap) {
    return res.json({ ok: false, error: 'Faltan surveyId o colMap' });
  }
  if (!colMap.gen || !colMap.edad || !colMap.prov) {
    return res.json({ ok: false, error: 'colMap debe incluir gen, edad y prov' });
  }

  // Guardar config
  syncConfig = {
    surveyId: String(surveyId),
    colMap,
    muestraId,
    intervalMinutes: Math.max(5, parseInt(intervalMinutes) || 15),
  };

  // Limpiar timer anterior
  if (syncTimer) { clearInterval(syncTimer); syncTimer = null; }

  // Precachear choices
  try { await buildChoiceCache(surveyId); } catch (_) {}

  // Sync inmediato
  syncActive = true;
  runSync().catch(e => console.error('[sync/config → runSync]', e.message));

  // Timer periódico
  const ms = syncConfig.intervalMinutes * 60 * 1000;
  syncTimer = setInterval(() => {
    runSync().catch(e => console.error('[sync timer]', e.message));
  }, ms);

  // Persistir config para sobrevivir reinicios
  await saveSyncConfigToFirebase(syncConfig);

  console.log(`[sync/config] Configurado: survey ${surveyId}, cada ${syncConfig.intervalMinutes} min`);
  res.json({
    ok:      true,
    message: `Sync configurado · cada ${syncConfig.intervalMinutes} min`,
  });
});

// POST /sync/now — fuerza sync inmediato
app.post('/sync/now', async (req, res) => {
  if (!syncConfig) return res.json({ ok: false, error: 'Sin configuración. Usá /sync/config primero.' });
  try {
    await runSync();
    res.json({ ok: true, lastSync: lastSyncTime });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// POST /sync/stop — detiene polling
app.post('/sync/stop', (req, res) => {
  if (syncTimer) { clearInterval(syncTimer); syncTimer = null; }
  syncActive = false;
  console.log('[sync/stop] Polling detenido');
  res.json({ ok: true });
});

// ──────────────────────────────────────────
// START
// ──────────────────────────────────────────
app.listen(PORT, async () => {
  console.log(`\n  Pulso Sync Server — puerto ${PORT}`);
  console.log(`  SM Token:  ${SM_TOKEN ? '✓ configurado' : '✗ FALTA SM_TOKEN'}`);
  console.log(`  Firebase:  ${fbReady ? '✓ conectado' : '✗ no disponible'}\n`);
  // Restaurar config guardada (sobrevive reinicios de Railway)
  await restoreSyncConfigFromFirebase();
});
