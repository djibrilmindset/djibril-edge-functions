// === AGENT MANAGER CRON — V1 ===
// Audit automatique du chatbot toutes les 1h30
// Analyse les conversations, détecte les problèmes, met à jour les patterns
// Boucle: Dashboard → Agent Manager → Chatbot → Conversations → Dashboard

import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';


// ── SECRETS (09/10) : rien en dur. Les valeurs vivaient en clair ici (JWT service_role
// du projet + clé Mistral) — dossier ~/agence-ia/state/SECU-0910/DOSSIER.md.
// Elles sont lues dans l'environnement Deno ; sans elles la fonction refuse de
// démarrer (fail-closed) au lieu de tourner avec un secret exposé.
const SUPABASE_URL = Deno.env.get('AGENT_MANAGER_SUPABASE_URL') ?? '';
const SUPABASE_SERVICE_KEY = Deno.env.get('AGENT_MANAGER_SUPABASE_SERVICE_KEY') ?? '';
const MISTRAL_KEY = Deno.env.get('AGENT_MANAGER_MISTRAL_KEY') ?? '';

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  throw new Error('[AGENT-MANAGER] AGENT_MANAGER_SUPABASE_URL / _SERVICE_KEY manquants (fail-closed)');
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

interface AuditResult {
  total_conversations: number;
  problems_found: string[];
  patterns_to_update: Array<{pattern: string; action: string; reason: string}>;
  quality_score: number;
  recommendations: string[];
}

// === AUDIT 1: Détecter les répétitions ===
async function auditRepetitions(hours: number = 2): Promise<string[]> {
  const since = new Date(Date.now() - hours * 60 * 60 * 1000).toISOString();
  const { data: conversations } = await supabase
    .from('conversation_history')
    .select('user_id, bot_response, created_at')
    .gte('created_at', since)
    .not('bot_response', 'is', null)
    .not('bot_response', 'eq', '__PENDING__')
    .not('bot_response', 'eq', '__ADMIN_TAKEOVER__')
    .order('created_at', { ascending: true });

  if (!conversations || conversations.length === 0) return [];

  const problems: string[] = [];
  const byUser: Record<string, string[]> = {};

  for (const conv of conversations) {
    if (!conv.bot_response || conv.bot_response.startsWith('__')) continue;
    if (!byUser[conv.user_id]) byUser[conv.user_id] = [];
    byUser[conv.user_id].push(conv.bot_response);
  }

  for (const [userId, responses] of Object.entries(byUser)) {
    for (let i = 1; i < responses.length; i++) {
      const sim = similarity(responses[i], responses[i-1]);
      if (sim > 0.6) {
        problems.push(`RÉPÉTITION user=${userId}: "${responses[i].substring(0, 50)}" ≈ "${responses[i-1].substring(0, 50)}" (sim=${sim.toFixed(2)})`);
      }
    }
  }
  return problems;
}

// === AUDIT 2: Détecter les phrases coupées ===
async function auditTruncated(hours: number = 2): Promise<string[]> {
  const since = new Date(Date.now() - hours * 60 * 60 * 1000).toISOString();
  const { data: conversations } = await supabase
    .from('conversation_history')
    .select('user_id, bot_response, created_at')
    .gte('created_at', since)
    .not('bot_response', 'is', null);

  if (!conversations) return [];
  const problems: string[] = [];

  const trailingWords = /\b(que|qui|les|des|un|une|le|la|de|du|et|ou|mais|car|si|par|pour|dans|sur|avec|en|au|aux|pas|plus|très|trop|genre|exemple)\s*$/i;

  for (const conv of conversations) {
    if (!conv.bot_response || conv.bot_response.startsWith('__')) continue;
    if (trailingWords.test(conv.bot_response)) {
      problems.push(`PHRASE COUPÉE user=${conv.user_id}: "${conv.bot_response}"`);
    }
  }
  return problems;
}

// === AUDIT 3: Détecter le coach-speak ===
async function auditCoachSpeak(hours: number = 2): Promise<string[]> {
  const since = new Date(Date.now() - hours * 60 * 60 * 1000).toISOString();
  const { data: conversations } = await supabase
    .from('conversation_history')
    .select('user_id, bot_response, created_at')
    .gte('created_at', since)
    .not('bot_response', 'is', null);

  if (!conversations) return [];
  const problems: string[] = [];

  const coachPatterns = [
    /ça montre que/i, /t.es (sur la bonne voie|prêt|capable)/i,
    /à ta portée/i, /c.est un (premier |bon )?pas/i,
    /t.as déjà la réponse/i, /tout à ton honneur/i,
    /chapeau/i, /bravo/i, /belle (démarche|initiative)/i,
    /n.hésite pas/i, /je suis là pour/i, /je comprends ta situation/i,
    /c.est un vrai challenge/i, /j.apprécie ta transparence/i,
    /c.est courageux/i, /merci de partager/i,
  ];

  for (const conv of conversations) {
    if (!conv.bot_response || conv.bot_response.startsWith('__')) continue;
    for (const pat of coachPatterns) {
      if (pat.test(conv.bot_response)) {
        problems.push(`COACH-SPEAK user=${conv.user_id}: "${conv.bot_response.substring(0, 60)}" → match: ${pat.source}`);
        break;
      }
    }
  }
  return problems;
}

// === AUDIT 4: Détecter les questions non-répondues ===
async function auditUnanswered(hours: number = 2): Promise<string[]> {
  const since = new Date(Date.now() - hours * 60 * 60 * 1000).toISOString();
  const { data: conversations } = await supabase
    .from('conversation_history')
    .select('user_id, user_message, bot_response, created_at')
    .gte('created_at', since)
    .not('bot_response', 'is', null)
    .order('created_at', { ascending: true });

  if (!conversations) return [];
  const problems: string[] = [];

  for (const conv of conversations) {
    if (!conv.user_message || !conv.bot_response || conv.bot_response.startsWith('__')) continue;
    // Si le user pose une question directe et le bot esquive
    const userAsks = /\?|c.est quoi|comment|combien|pourquoi|qu.est.ce|tu proposes? quoi/i.test(conv.user_message);
    if (userAsks) {
      const botAnswers = /oui|non|c.est|€|euro|jour|mois|semaine|lien|regarde|tiens|http/i.test(conv.bot_response);
      const botAsksBack = /\?/.test(conv.bot_response);
      if (!botAnswers && botAsksBack) {
        // Le bot a juste renvoyé une question sans répondre
        problems.push(`ESQUIVE user=${conv.user_id}: Q="${conv.user_message.substring(0, 50)}" → R="${conv.bot_response.substring(0, 50)}" (question sans réponse)`);
      }
    }
  }
  return problems;
}

// === AUDIT 5: Détecter les fuites de termes internes ===
async function auditLeaks(hours: number = 2): Promise<string[]> {
  const since = new Date(Date.now() - hours * 60 * 60 * 1000).toISOString();
  const { data: conversations } = await supabase
    .from('conversation_history')
    .select('user_id, bot_response, created_at')
    .gte('created_at', since)
    .not('bot_response', 'is', null);

  if (!conversations) return [];
  const problems: string[] = [];

  const leakPatterns = [
    /Adam/i, /Pellabère/i, /Cialdini/i, /récipient/i, /encre (passive|active)/i,
    /ACCUEIL|EXPLORER|CREUSER|RÉVÉLER|QUALIFIER|CLOSER/,
    /funnel|pattern|debounce|webhook|endpoint/i,
    /\{\{/,  // Variables ManyChat
    /\(\d+\s*chars?\)/i,  // Debug markers
    /language model|LLM|GPT|Claude|Mistral/i,
  ];

  for (const conv of conversations) {
    if (!conv.bot_response || conv.bot_response.startsWith('__')) continue;
    for (const pat of leakPatterns) {
      if (pat.test(conv.bot_response)) {
        problems.push(`FUITE user=${conv.user_id}: "${conv.bot_response.substring(0, 60)}" → leak: ${pat.source}`);
        break;
      }
    }
  }
  return problems;
}

// === AUDIT 6: Score qualité global ===
async function calculateQualityScore(hours: number = 2): Promise<{score: number; details: Record<string, number>}> {
  const since = new Date(Date.now() - hours * 60 * 60 * 1000).toISOString();
  const { data: conversations } = await supabase
    .from('conversation_history')
    .select('user_id, user_message, bot_response, created_at')
    .gte('created_at', since)
    .not('bot_response', 'is', null)
    .not('bot_response', 'eq', '__PENDING__');

  if (!conversations || conversations.length === 0) return { score: -1, details: {} };

  let total = 0;
  let good = 0;
  const details: Record<string, number> = {
    short_enough: 0, natural_tone: 0, uses_mirror: 0,
    answers_question: 0, no_coach_speak: 0, no_repeat: 0
  };

  for (const conv of conversations) {
    if (!conv.bot_response || conv.bot_response.startsWith('__')) continue;
    total++;
    let thisScore = 0;

    // Court (< 150 chars)
    if (conv.bot_response.length < 150) { thisScore++; details.short_enough++; }
    // Ton naturel (contractions)
    if (/j'|t'|c'|y'|l'/.test(conv.bot_response)) { thisScore++; details.natural_tone++; }
    // Utilise le miroir (reprend les mots du user)
    if (conv.user_message) {
      const userWords = conv.user_message.toLowerCase().split(/\s+/).filter((w: string) => w.length > 3);
      const botLow = conv.bot_response.toLowerCase();
      if (userWords.some((w: string) => botLow.includes(w))) { thisScore++; details.uses_mirror++; }
    }
    // Répond aux questions
    if (conv.user_message && /\?/.test(conv.user_message)) {
      if (!/^\s*(Genre|Et du coup|Ah ouais|Développe)\s*\??$/i.test(conv.bot_response)) {
        thisScore++; details.answers_question++;
      }
    }
    // Pas de coach-speak
    if (!/ça montre|bonne voie|à ta portée|bravo|chapeau|n.hésite pas/i.test(conv.bot_response)) {
      thisScore++; details.no_coach_speak++;
    }

    if (thisScore >= 3) good++;
  }

  const score = total > 0 ? Math.round((good / total) * 100) : -1;
  return { score, details };
}

// === AGENT MANAGER: Audit complet + log résultats ===
async function runFullAudit(): Promise<AuditResult> {
  console.log('[AGENT-MANAGER] 🔍 Démarrage audit complet...');

  const [repetitions, truncated, coachSpeak, unanswered, leaks, quality] = await Promise.all([
    auditRepetitions(2),
    auditTruncated(2),
    auditCoachSpeak(2),
    auditUnanswered(2),
    auditLeaks(2),
    calculateQualityScore(2),
  ]);

  const allProblems = [...repetitions, ...truncated, ...coachSpeak, ...unanswered, ...leaks];

  const result: AuditResult = {
    total_conversations: quality.score >= 0 ? Object.values(quality.details).reduce((a, b) => Math.max(a, b), 0) : 0,
    problems_found: allProblems,
    patterns_to_update: [],
    quality_score: quality.score,
    recommendations: [],
  };

  // Recommandations basées sur les problèmes
  if (repetitions.length > 0) result.recommendations.push(`${repetitions.length} répétitions détectées — vérifier isTooSimilar threshold`);
  if (truncated.length > 0) result.recommendations.push(`${truncated.length} phrases coupées — augmenter MAX_TOKENS ou troncature`);
  if (coachSpeak.length > 0) result.recommendations.push(`${coachSpeak.length} coach-speak — ajouter patterns dans clean()`);
  if (unanswered.length > 0) result.recommendations.push(`${unanswered.length} questions esquivées — renforcer RÈGLE #1`);
  if (leaks.length > 0) result.recommendations.push(`${leaks.length} fuites internes — ajouter patterns dans clean()`);
  if (quality.score < 70) result.recommendations.push(`Score qualité ${quality.score}/100 — audit approfondi recommandé`);

  // Sauvegarder le rapport d'audit en DB
  try {
    await supabase.from('conversation_history').insert({
      platform: 'system',
      user_id: 'AGENT_MANAGER',
      user_message: `AUDIT ${new Date().toISOString().substring(0, 16)}`,
      bot_response: JSON.stringify({
        score: quality.score,
        problems: allProblems.length,
        details: quality.details,
        recommendations: result.recommendations,
      }).substring(0, 2000),
    });
  } catch (e) {
    console.error('[AGENT-MANAGER] Erreur save audit:', e);
  }

  console.log(`[AGENT-MANAGER] ✅ Audit terminé: Score=${quality.score}/100, Problèmes=${allProblems.length}`);
  console.log(`[AGENT-MANAGER] Détails: ${JSON.stringify(quality.details)}`);
  if (allProblems.length > 0) {
    console.log(`[AGENT-MANAGER] 🚨 Problèmes:`);
    for (const p of allProblems.slice(0, 10)) console.log(`  - ${p}`);
  }
  for (const r of result.recommendations) console.log(`[AGENT-MANAGER] 💡 ${r}`);

  return result;
}

// === SIMILARITY (copie de la fonction du chatbot) ===
function similarity(a: string, b: string): number {
  const a1 = a.toLowerCase().replace(/[^a-zà-ÿ0-9\s]/g, '');
  const b1 = b.toLowerCase().replace(/[^a-zà-ÿ0-9\s]/g, '');
  const wordsA = new Set(a1.split(/\s+/));
  const wordsB = new Set(b1.split(/\s+/));
  const intersection = [...wordsA].filter(w => wordsB.has(w)).length;
  const union = new Set([...wordsA, ...wordsB]).size;
  return union > 0 ? intersection / union : 0;
}

// === HANDLER ===
Deno.serve(async (req: Request) => {
  try {
    if (req.method === 'OPTIONS') {
      return new Response(null, { headers: { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type' } });
    }

    const result = await runFullAudit();

    return new Response(JSON.stringify({
      status: 'ok',
      audit: {
        score: result.quality_score,
        problems: result.problems_found.length,
        recommendations: result.recommendations,
      }
    }), {
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
    });
  } catch (e: any) {
    console.error('[AGENT-MANAGER] Error:', e.message);
    return new Response(JSON.stringify({ error: e.message }), { status: 500 });
  }
});
