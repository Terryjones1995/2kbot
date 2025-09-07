#!/usr/bin/env node
/**
 * Consolidated & Updated Comp Verification Bot - index.js
 * - Default MIN_GAMES bumped to 100
 * - Duplicate-key insert fallback now creates admin pending approvals & DM
 * - Admin approve flow now reassigns existing conflicting tag before updating
 *
 * Node 18+, discord.js v14, Supabase, OpenAI, Sharp
 */

require('dotenv').config();

const {
  Client,
  GatewayIntentBits,
  Partials,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  EmbedBuilder,
  PermissionsBitField,
  REST,
  Routes,
  SlashCommandBuilder,
  ChannelType
} = require('discord.js');

const sharp = require('sharp');
const { createClient } = require('@supabase/supabase-js');
const cron = require('node-cron');
const OpenAI = require('openai');
const crypto = require('crypto');
const fetch = globalThis.fetch || require('node-fetch');

// ================= ENV =================
const TOKEN = process.env.DISCORD_TOKEN;
const CLIENT_ID = process.env.CLIENT_ID;
const GUILD_ID = process.env.GUILD_ID || '';
const TEST_GUILD_ID = process.env.TEST_GUILD_ID;
const CHANNEL_NAME = process.env.BOT_CHANNEL_NAME || 'comp-verification';
const ROLE_ID_ENV = process.env.ROLE_ID || null;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
// DEFAULTS: MIN_GAMES default bumped to 100 per your request
const MIN_GAMES = Number(process.env.MIN_GAMES || 100);
const MIN_WIN_PCT = Number(process.env.MIN_WIN_PCT || 80.0);
const REVERIFY_DAYS = Number(process.env.REVERIFY_DAYS || 30);
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const PREFERRED_OPENAI_MODEL = process.env.OPENAI_MODEL || 'gpt-5-mini';
const OCR_DEBUG = (process.env.OCR_DEBUG || 'false').toLowerCase() === 'true';

// Channel to post player cards (hardcoded fallback updated per your request)
const PLAYER_CARD_CHANNEL_ID = process.env.PLAYER_CARD_CHANNEL_ID || '1414103417453805681';

// fallback role id (production role you provided). Can still be overridden with env var FALLBACK_ROLE_ID
const FALLBACK_ROLE_ID = process.env.FALLBACK_ROLE_ID || '1414033567067144202';

// Admin user to receive approvals (provided fallback ID)
const ADMIN_USER_ID = process.env.ADMIN_USER_ID || '637758147330572349';

const VERIF_CHANNEL_EMOJI = process.env.VERIF_CHANNEL_EMOJI || '✅';
const LOG_CHANNEL_EMOJI = process.env.LOG_CHANNEL_EMOJI || '📜';
const LOG_CHANNEL_NAME = process.env.LOG_CHANNEL_NAME || 'comp-logs';
const CATEGORY_NAME = process.env.CATEGORY_NAME || 'comp';

if (!TOKEN || !CLIENT_ID || !TEST_GUILD_ID || !SUPABASE_URL || !SUPABASE_KEY) {
  console.error('Missing required env vars. Required: DISCORD_TOKEN, CLIENT_ID, TEST_GUILD_ID, SUPABASE_URL, SUPABASE_KEY');
  process.exit(1);
}
if (!OPENAI_API_KEY) {
  console.warn('OPENAI_API_KEY not provided. Bot will require OpenAI to parse images.');
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
let openai = null;
let activeOpenAIModel = null;

if (OPENAI_API_KEY) {
  try {
    openai = new OpenAI({ apiKey: OPENAI_API_KEY });
  } catch (err) {
    console.warn('Failed to init OpenAI client:', err?.message || err);
    openai = null;
  }
}

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.DirectMessages,
    GatewayIntentBits.MessageContent
  ],
  partials: [Partials.Channel]
});

// prevent duplicate ready runs
let readyRan = false;

// pending approvals store (in-memory)
const pendingApprovals = new Map(); // reqId -> { userId, guildId, prevTag, newTag, newPlatform, oldImage, newImage, otherUserId, otherImage }

function makeChannelCreationName(emoji, baseName) {
  const sanitized = String(baseName).trim().replace(/\s+/g, '-').toLowerCase();
  return `${emoji}-${sanitized}`.slice(0, 100);
}
const VERIF_CREATE_NAME = makeChannelCreationName(VERIF_CHANNEL_EMOJI, CHANNEL_NAME);
const LOG_CREATE_NAME = makeChannelCreationName(LOG_CHANNEL_EMOJI, LOG_CHANNEL_NAME);
const CATEGORY_CREATE_NAME = makeChannelCreationName(VERIF_CHANNEL_EMOJI, CATEGORY_NAME);

function normalizeName(s) {
  if (!s) return '';
  return String(s).toLowerCase().replace(/[^a-z0-9]/g, '');
}
function fuzzyNameMatch(a, b) {
  const na = normalizeName(a);
  const nb = normalizeName(b);
  if (!na || !nb) return false;
  if (na === nb) return true;
  if (na.includes(nb) || nb.includes(na)) return true;
  return false;
}

// register /player view
async function registerCommands() {
  const commands = [
    new SlashCommandBuilder()
      .setName('player')
      .setDescription('Player related commands')
      .addSubcommand(sub =>
        sub.setName('view')
          .setDescription('View a player\'s saved stats')
          .addUserOption(opt => opt.setName('user').setDescription('User to view').setRequired(false))
      )
      .toJSON()
  ];

  const rest = new REST({ version: '10' }).setToken(TOKEN);

  try {
    await rest.put(Routes.applicationCommands(CLIENT_ID), { body: commands });
    console.log('Registered GLOBAL commands (may take ~1 hour to propagate).');
  } catch (err) {
    console.warn('Global command register failed:', err?.message || err);
  }

  try {
    await rest.put(Routes.applicationGuildCommands(CLIENT_ID, TEST_GUILD_ID), { body: commands });
    console.log('Registered guild commands to TEST_GUILD_ID:', TEST_GUILD_ID);
  } catch (err) {
    console.warn('Failed to register to TEST_GUILD_ID:', err?.message || err);
  }

  if (GUILD_ID) {
    try {
      await rest.put(Routes.applicationGuildCommands(CLIENT_ID, GUILD_ID), { body: commands });
      console.log('Registered guild commands to production GUILD_ID:', GUILD_ID);
    } catch (err) {
      console.warn('Prod guild registration failed (optional):', err?.message || err);
    }
  }
}

// supabase small helpers
async function getSavedChannelId(guildId) {
  try {
    const { data, error } = await supabase.from('comp_settings').select('channel_id').eq('guild_id', guildId).maybeSingle();
    if (!error && data?.channel_id) return data.channel_id;
  } catch (err) {
    if (OCR_DEBUG) console.warn('getSavedChannelId error:', err.message);
  }
  return null;
}
async function saveChannelId(guildId, channelId) {
  try {
    await supabase.from('comp_settings').upsert({ guild_id: guildId, channel_id: channelId }, { onConflict: ['guild_id'] });
  } catch (err) {
    console.warn('Failed to persist channel_id to comp_settings:', err?.message || err);
  }
}
async function getSavedLogChannelId(guildId) {
  try {
    const { data, error } = await supabase.from('comp_settings').select('log_channel_id').eq('guild_id', guildId).maybeSingle();
    if (!error && data?.log_channel_id) return data.log_channel_id;
  } catch (err) {
    if (OCR_DEBUG) console.warn('getSavedLogChannelId error:', err.message);
  }
  return null;
}
async function saveLogChannelId(guildId, channelId) {
  try {
    await supabase.from('comp_settings').upsert({ guild_id: guildId, log_channel_id: channelId }, { onConflict: ['guild_id'] });
  } catch (err) {
    console.warn('Failed to persist log_channel_id to comp_settings:', err?.message || err);
  }
}
async function getSavedCategoryId(guildId) {
  try {
    const { data, error } = await supabase.from('comp_settings').select('category_id').eq('guild_id', guildId).maybeSingle();
    if (!error && data?.category_id) return data.category_id;
  } catch (err) {
    if (OCR_DEBUG) console.warn('getSavedCategoryId error:', err.message);
  }
  return null;
}
async function saveCategoryId(guildId, categoryId) {
  try {
    await supabase.from('comp_settings').upsert({ guild_id: guildId, category_id: categoryId }, { onConflict: ['guild_id'] });
  } catch (err) {
    console.warn('Failed to persist category_id to comp_settings:', err?.message || err);
  }
}

// channel/category helpers (same as before, uses parent category when creating)
async function fetchPinsSafe(channel) {
  try {
    if (typeof channel.messages.fetchPins === 'function') {
      const pins = await channel.messages.fetchPins();
      if (Array.isArray(pins)) return pins;
      if (pins && typeof pins.values === 'function') return Array.from(pins.values());
      if (pins && typeof pins[Symbol.iterator] === 'function') return Array.from(pins);
      return [];
    }
    if (typeof channel.messages.fetchPinned === 'function') {
      const pins = await channel.messages.fetchPinned();
      if (Array.isArray(pins)) return pins;
      if (pins && typeof pins.values === 'function') return Array.from(pins.values());
      if (pins && typeof pins[Symbol.iterator] === 'function') return Array.from(pins);
      return [];
    }
    const msgs = await channel.messages.fetch({ limit: 200 });
    if (!msgs) return [];
    return Array.from(msgs.filter(m => m.pinned).values());
  } catch (err) {
    console.warn('fetchPinsSafe error:', err?.message || err);
    return [];
  }
}

async function findOrCreateCategory(guild) {
  try {
    const saved = await getSavedCategoryId(guild.id);
    if (saved) {
      const cat = await guild.channels.fetch(saved).catch(() => null);
      if (cat && cat.type === ChannelType.GuildCategory) return cat;
      else await saveCategoryId(guild.id, null);
    }
  } catch (e) { if (OCR_DEBUG) console.warn('Saved category check failed', e?.message || e); }

  let all;
  try { all = await guild.channels.fetch(); } catch (e) { all = guild.channels.cache; }
  const matches = Array.from(all.values()).filter(c => c.type === ChannelType.GuildCategory && (fuzzyNameMatch(c.name, CATEGORY_NAME) || fuzzyNameMatch(c.name, CATEGORY_CREATE_NAME)));
  if (matches.length > 0) {
    const keep = matches[0];
    await saveCategoryId(guild.id, keep.id);
    return keep;
  }

  try {
    const created = await guild.channels.create({
      name: CATEGORY_CREATE_NAME,
      type: ChannelType.GuildCategory,
      reason: 'Create comp verification category'
    });
    await saveCategoryId(guild.id, created.id);
    console.log('Created category', created.name, 'in guild', guild.id);
    return created;
  } catch (err) {
    if (OCR_DEBUG) console.warn('Could not create category (maybe missing perm):', err?.message || err);
    return null;
  }
}

async function findOrCreateChannel(guild) {
  try {
    const saved = await getSavedChannelId(guild.id);
    if (saved) {
      const ch = await guild.channels.fetch(saved).catch(() => null);
      if (ch && ch.type === ChannelType.GuildText) {
        try {
          await ch.permissionOverwrites.edit(guild.roles.everyone.id, { SendMessages: false, SendMessagesInThreads: false });
        } catch (e) {}
        return ch;
      } else {
        await saveChannelId(guild.id, null);
      }
    }
  } catch (err) {
    if (OCR_DEBUG) console.warn('Saved channel check failed:', err?.message || err);
  }

  let allChannels;
  try {
    allChannels = await guild.channels.fetch();
  } catch (err) {
    allChannels = guild.channels.cache;
  }

  const matches = allChannels.filter(ch => {
    if (!ch || ch.type !== ChannelType.GuildText) return false;
    if (fuzzyNameMatch(ch.name, CHANNEL_NAME) || fuzzyNameMatch(ch.name, VERIF_CREATE_NAME)) return true;
    if (ch.topic && typeof ch.topic === 'string' && ch.topic.toLowerCase().includes('comp verification')) return true;
    const lower = (ch.name || '').toLowerCase();
    if (lower.includes('comp') && (lower.includes('verif') || lower.includes('verification'))) return true;
    return false;
  });

  if (matches.size > 1) {
    // Avoid BigInt arithmetic in sort comparator (causes "Cannot convert a BigInt value to a number")
    const sorted = Array.from(matches.values()).sort((a, b) => a.id.localeCompare(b.id));
    const keep = sorted[0];
    for (const other of sorted.slice(1)) {
      try { await other.delete('Auto-remove duplicate verification channel'); } catch (e) { if (OCR_DEBUG) console.warn('delete dup verif failed', e?.message || e); }
    }
    await saveChannelId(guild.id, keep.id);
    return keep;
  }

  const channel = matches.first();
  if (channel) {
    await saveChannelId(guild.id, channel.id);
    return channel;
  }

  let category = await findOrCreateCategory(guild).catch(() => null);
  try {
    const created = await guild.channels.create({
      name: VERIF_CREATE_NAME,
      type: ChannelType.GuildText,
      topic: 'Comp verification for NBA2K26. Click Verify to start. Upload screenshots via DM. Make sure the **Games Played** number and **Win percentage** are visible.',
      parent: category ? category.id : undefined,
      permissionOverwrites: [
        {
          id: guild.roles.everyone.id,
          deny: [PermissionsBitField.Flags.SendMessages, PermissionsBitField.Flags.SendMessagesInThreads]
        }
      ],
      reason: 'Create comp verification channel'
    });
    await saveChannelId(guild.id, created.id);
    return created;
  } catch (err) {
    if (OCR_DEBUG) console.warn('Failed to create verification channel:', err?.message || err);
    return null;
  }
}

async function findOrCreateLogChannel(guild) {
  try {
    const saved = await getSavedLogChannelId(guild.id);
    if (saved) {
      const ch = await guild.channels.fetch(saved).catch(() => null);
      if (ch && ch.type === ChannelType.GuildText) return ch;
      else await saveLogChannelId(guild.id, null);
    }
  } catch (err) { if (OCR_DEBUG) console.warn('Saved log channel check failed:', err?.message || err); }

  let allChannels;
  try { allChannels = await guild.channels.fetch(); } catch (err) { allChannels = guild.channels.cache; }

  const matches = allChannels.filter(ch => {
    if (!ch || ch.type !== ChannelType.GuildText) return false;
    if (fuzzyNameMatch(ch.name, LOG_CHANNEL_NAME) || fuzzyNameMatch(ch.name, LOG_CREATE_NAME)) return true;
    const lower = (ch.name || '').toLowerCase();
    if (lower.includes('log') || lower.includes('logs')) return true;
    return false;
  });

  if (matches.size > 1) {
    // Avoid BigInt arithmetic in sort comparator
    const sorted = Array.from(matches.values()).sort((a, b) => a.id.localeCompare(b.id));
    const keep = sorted[0];
    for (const other of sorted.slice(1)) {
      try { await other.delete('Auto-remove duplicate log channel'); } catch (e) { if (OCR_DEBUG) console.warn('failed delete log dup', e?.message || e); }
    }
    await saveLogChannelId(guild.id, keep.id);
    return keep;
  }

  const channel = matches.first();
  if (channel) {
    await saveLogChannelId(guild.id, channel.id);
    return channel;
  }

  const category = await findOrCreateCategory(guild).catch(() => null);
  try {
    const created = await guild.channels.create({
      name: LOG_CREATE_NAME,
      type: ChannelType.GuildText,
      topic: 'Comp verification logs. Internal bot logs for verification events.',
      parent: category ? category.id : undefined,
      permissionOverwrites: [
        {
          id: guild.roles.everyone.id,
          deny: [PermissionsBitField.Flags.SendMessages]
        }
      ],
      reason: 'Create comp verification log channel'
    });
    await saveLogChannelId(guild.id, created.id);
    console.log('Created log channel', created.name, 'in guild', guild.id);
    return created;
  } catch (err) {
    if (OCR_DEBUG) console.warn('Could not create log channel (maybe missing perms):', err?.message || err);
    return null;
  }
}

async function logToGuild(guild, title, description) {
  try {
    if (!guild) return;
    const logCh = await findOrCreateLogChannel(guild).catch(() => null);
    if (logCh) {
      const embed = new EmbedBuilder()
        .setTitle(title)
        .setDescription(description)
        .setColor(0xF1C40F)
        .setTimestamp();
      await logCh.send({ embeds: [embed] }).catch(e => { if (OCR_DEBUG) console.warn('Could not send to log channel', e?.message || e); });
      return;
    }
    const savedVerifId = await getSavedChannelId(guild.id).catch(() => null);
    let verifCh = null;
    if (savedVerifId) verifCh = await guild.channels.fetch(savedVerifId).catch(() => null);
    if (!verifCh) {
      const fetched = await guild.channels.fetch().catch(() => guild.channels.cache);
      const candidate = Array.from((fetched || guild.channels.cache).values()).find(c => c.type === ChannelType.GuildText && (fuzzyNameMatch(c.name, CHANNEL_NAME) || fuzzyNameMatch(c.name, VERIF_CREATE_NAME)));
      verifCh = candidate || null;
    }
    if (verifCh) {
      const embed = new EmbedBuilder()
        .setTitle(title)
        .setDescription(description)
        .setColor(0xF1C40F)
        .setTimestamp();
      await verifCh.send({ embeds: [embed] }).catch(e => { if (OCR_DEBUG) console.warn('Fallback log send failed:', e?.message || e); });
    } else {
      if (OCR_DEBUG) console.warn('No channel available to send logs to for guild', guild.id);
    }
  } catch (err) {
    if (OCR_DEBUG) console.warn('logToGuild failed:', err?.message || err);
  }
}

// image helpers
async function downloadImageToBuffer(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to download image: ${res.status}`);
  return Buffer.from(await res.arrayBuffer());
}
async function computeImageHash(buffer) {
  return crypto.createHash('sha256').update(buffer).digest('hex');
}
async function preprocessForOCR(buffer) {
  const cleaned = await sharp(buffer)
    .resize(1600, null, { fit: 'inside' })
    .grayscale()
    .sharpen()
    .modulate({ brightness: 1.05, saturation: 1.0 })
    .toBuffer();

  const binary = await sharp(cleaned)
    .threshold(140)
    .toBuffer();

  return { cleaned, binary };
}

// OpenAI probing + parsing (unchanged except extended prompt for tag/platform)
async function probeOpenAIModel(modelName) {
  if (!openai) return false;
  try {
    const resp = await openai.responses.create({
      model: modelName,
      input: [{ role: 'user', content: [{ type: 'input_text', text: 'Respond with JSON: {"ping":"pong"}' }] }],
      temperature: 0,
      max_output_tokens: 30
    });
    let text = '';
    if (resp.output_text) text = resp.output_text;
    else if (resp.output && resp.output.length) {
      text = resp.output.map(o => {
        if (o.content && Array.isArray(o.content)) return o.content.map(c => c.text || '').join('');
        return o.text || '';
      }).join('');
    }
    if (!text) throw new Error('empty');
    return true;
  } catch (err) {
    if (OCR_DEBUG) console.warn(`probe failed for ${modelName}:`, err?.message || err);
    return false;
  }
}
async function chooseWorkingOpenAIModel(preferred) {
  const candidates = [
    preferred,
    'gpt-5-mini',
    'gpt-4o-mini',
    'gpt-4o',
    'gpt-5',
    'gpt-5-thinking'
  ].filter(Boolean);

  for (const m of candidates) {
    try {
      const ok = await probeOpenAIModel(m);
      if (ok) {
        console.log('Selected OpenAI model:', m);
        return m;
      }
    } catch (err) {
      if (OCR_DEBUG) console.warn('probe error for', m, err?.message || err);
    }
  }
  return null;
}

async function parseWithOpenAI(imageUrl) {
  if (!openai) throw new Error('OpenAI client not initialized');
  if (!activeOpenAIModel) throw new Error('No active OpenAI model selected');

  const instructionText =
`You are given an image of an NBA2K Stats screen. Extract EXACTLY one JSON object with these keys:
{"games_played": <int|null>, "win_pct": <float|null>, "points": <int|null>, "rebounds": <int|null>, "assists": <int|null>, "player_tag": <string|null>, "platform": <string|null>}
player_tag is the PSN or Gamertag shown on the screen. platform is PSN, Xbox, or PC when visible. If a field is unreadable, use null. Return ONLY the JSON object, with no explanation. Example:
{"games_played":147,"win_pct":70.1,"points":1155,"rebounds":155,"assists":336,"player_tag":"brockhogg","platform":"PSN"}`;

  const requestBody = {
    model: activeOpenAIModel,
    input: [
      {
        role: 'user',
        content: [
          { type: 'input_text', text: instructionText },
          { type: 'input_image', image_url: imageUrl }
        ]
      }
    ],
    temperature: 0.0,
    max_output_tokens: 800
  };

  if (OCR_DEBUG) console.log('OpenAI parse request using model', activeOpenAIModel);

  const resp = await openai.responses.create(requestBody);

  let textOutput = null;
  if (resp.output_text) textOutput = resp.output_text;
  else if (resp.output && resp.output.length) {
    const parts = resp.output.map(item => {
      if (item.content && Array.isArray(item.content)) {
        return item.content.map(c => (c?.text ?? '')).join('');
      }
      return (item?.text ?? '');
    }).filter(Boolean);
    textOutput = parts.join('\n').trim();
  }

  if (!textOutput) throw new Error('No textual output from OpenAI response');

  let jsonStr = textOutput.replace(/```json/g, '').replace(/```/g, '').trim();
  const jm = jsonStr.match(/\{[\s\S]*\}/);
  if (jm) jsonStr = jm[0];
  const parsed = JSON.parse(jsonStr);

  return {
    source: `openai:${activeOpenAIModel}`,
    raw_openai: textOutput,
    games_played: parsed.games_played != null ? parseInt(parsed.games_played, 10) : null,
    win_pct: parsed.win_pct != null ? parseFloat(parsed.win_pct) : null,
    points: parsed.points != null ? parseInt(parsed.points, 10) : null,
    rebounds: parsed.rebounds != null ? parseInt(parsed.rebounds, 10) : null,
    assists: parsed.assists != null ? parseInt(parsed.assists, 10) : null,
    // IMPORTANT: treat empty or whitespace-only tags as null
    player_tag: (parsed.player_tag != null && String(parsed.player_tag).trim().length > 0)
                ? String(parsed.player_tag).trim()
                : null,
    platform: parsed.platform != null ? String(parsed.platform).trim() : null
  };
}

async function parseImageStats(imageUrl) {
  if (!openai) throw new Error('OpenAI client not initialized');
  if (!activeOpenAIModel) throw new Error('No active OpenAI model selected');

  const buffer = await downloadImageToBuffer(imageUrl);
  const image_hash = await computeImageHash(buffer);
  await preprocessForOCR(buffer);

  const parsed = await parseWithOpenAI(imageUrl);
  parsed.image_hash = image_hash;
  return parsed;
}

// ================ Supabase helpers ================
// improved save: try insert, then fetch latest row for the user/guild for consistency
// added duplicate-key fallback to avoid crashing on concurrent insert races
async function saveVerificationRecord(record) {
  try {
    const insertResp = await supabase.from('comp_verifications').insert([record]).select().maybeSingle();
    if (insertResp.error) {
      // handle likely schema/column issues (existing fallback) and other errors below
      const msg = String(insertResp.error.message || '').toLowerCase();
      if (msg.includes('column') || msg.includes('does not exist') || msg.includes('could not find')) {
        // fallback to raw insert without .select()
        const safe = { ...record };
        delete safe.source;
        delete safe.raw_openai;
        delete safe.raw_ocr;
        await supabase.from('comp_verifications').insert([safe]);
        if (OCR_DEBUG) console.log('Inserted record using safe fallback (no select).');
        // attempt to fetch latest row
        const maybe = await getLatestRecord(record.user_id, record.guild_id);
        return maybe;
      }

      // Detect duplicate-key constraint from Supabase/Postgres and handle gracefully
      if (msg.includes('duplicate key') || msg.includes('violates unique constraint')) {
        // attempt to fetch the conflicting record
        try {
          const conflictQuery = await supabase.from('comp_verifications')
            .select()
            .eq('guild_id', record.guild_id)
            .eq('player_tag', record.player_tag)
            .order('created_at', { ascending: false })
            .limit(1);
          const conflictRecord = (!conflictQuery.error && conflictQuery.data && conflictQuery.data.length > 0) ? conflictQuery.data[0] : null;

          // Insert a fallback record that preserves the submission but avoids unique constraint by suffixing the player_tag
          const suffix = `__dup__${crypto.randomUUID().slice(0,8)}`;
          const alt = { ...record };
          alt.player_tag = (alt.player_tag ? String(alt.player_tag).slice(0, 120) : 'unknown') + suffix;
          alt.flagged = true;
          alt.flag_reason = `Duplicate tag conflict during insert; original tag: ${record.player_tag}`;
          if (alt.raw_openai === undefined) delete alt.raw_openai;
          if (alt.source === undefined) delete alt.source;

          const safeAlt = { ...alt };
          // ensure fields that could cause issues removed
          delete safeAlt.raw_ocr;

          const altResp = await supabase.from('comp_verifications').insert([safeAlt]).select().maybeSingle();
          if (!altResp.error && altResp.data) {
            // notify guild logs about the collision so admin can reconcile
            try {
              const guild = await client.guilds.fetch(record.guild_id).catch(() => null);
              if (guild) {
                await logToGuild(guild, 'Duplicate-key fallback saved', `A new submission for tag **${record.player_tag}** conflicted with an existing record. The submission was saved as **${alt.player_tag}** and flagged for admin review.`);
              }
            } catch (_) {}

            // --- NEW: create pending approval and DM admin for this fallback insertion ---
            try {
              const reqId = crypto.randomUUID();
              const pending = {
                userId: record.user_id,
                guildId: record.guild_id,
                prevTag: conflictRecord ? conflictRecord.player_tag : null,
                newTag: record.player_tag,
                newPlatform: record.platform || null,
                oldImage: conflictRecord ? conflictRecord.image_url : null,
                newImage: record.image_url || null,
                otherUserId: conflictRecord ? conflictRecord.user_id : null,
                otherImage: conflictRecord ? conflictRecord.image_url : null,
                altSavedTag: alt.player_tag // what was actually saved
              };
              pendingApprovals.set(reqId, pending);

              const adminUser = await client.users.fetch(ADMIN_USER_ID).catch(() => null);
              const adminEmbed = new EmbedBuilder()
                .setTitle('Duplicate-key fallback saved — admin attention required')
                .setDescription(`A new submission for tag **${record.player_tag}** conflicted with an existing record. The submission was automatically saved as **${alt.player_tag}** and flagged for admin review.\n\nIf you want to make the newly-saved submission the canonical tag, approve it. Otherwise, deny to keep the existing owner.`)
                .addFields(
                  { name: 'Guild', value: `<@${record.guild_id}> (${record.guild_id})`, inline: true },
                  { name: 'New submitter', value: `<@${record.user_id}>`, inline: true },
                  { name: 'Existing owner', value: conflictRecord ? `<@${conflictRecord.user_id}>` : 'None found', inline: true }
                )
                .setTimestamp();

              const comps = new ActionRowBuilder().addComponents(
                new ButtonBuilder().setCustomId(`admin_approve:${reqId}`).setLabel('Approve new submission').setStyle(ButtonStyle.Success),
                new ButtonBuilder().setCustomId(`admin_deny:${reqId}`).setLabel('Keep existing / Deny new').setStyle(ButtonStyle.Danger)
              );

              if (adminUser) {
                try {
                  await adminUser.send({ embeds: [adminEmbed], components: [comps] }).catch(() => { throw new Error('admin DM send failed'); });
                  if (pending.oldImage) await adminUser.send({ content: `Existing owner image for <@${conflictRecord.user_id}>: ${pending.oldImage}` }).catch(() => null);
                  if (pending.newImage) await adminUser.send({ content: `New submitter image: ${pending.newImage}` }).catch(() => null);
                } catch (e) {
                  // fail gracefully: admin DM failed; log to guild as fallback
                  if (OCR_DEBUG) console.warn('Failed to DM admin for duplicate-key fallback:', e?.message || e);
                  try { if (record.guild_id) { const g = await client.guilds.fetch(record.guild_id).catch(() => null); if (g) await logToGuild(g, 'Duplicate-key fallback - admin DM failed', `Could not DM admin for duplicate-key fallback for tag ${record.player_tag}`); } } catch (_) {}
                }
              } else {
                if (OCR_DEBUG) console.warn('Admin user not found for duplicate-key fallback', ADMIN_USER_ID);
              }
            } catch (e) {
              if (OCR_DEBUG) console.warn('Failed creating pending approval for duplicate-key fallback:', e?.message || e);
            }

            return altResp.data;
          } else {
            // if even fallback insert fails, rethrow original error
            throw insertResp.error;
          }
        } catch (e) {
          // final fallback: rethrow original error so caller can handle
          throw insertResp.error;
        }
      }

      // otherwise throw the insert error
      throw insertResp.error;
    }
    // InsertResp.data should be the inserted row - but to be safe we'll fetch latest
    const maybe = await getLatestRecord(record.user_id, record.guild_id);
    if (maybe) return maybe;
    return insertResp.data || null;
  } catch (err) {
    const emsg = String(err?.message || err).toLowerCase();
    // Keep previous safe fallback for column errors
    if (emsg.includes('column') || emsg.includes('does not exist') || emsg.includes('could not find')) {
      try {
        const safe = { ...record };
        delete safe.source;
        delete safe.raw_openai;
        delete safe.raw_ocr;
        await supabase.from('comp_verifications').insert([safe]);
        if (OCR_DEBUG) console.log('Inserted record using safe fallback (caught).');
        const maybe = await getLatestRecord(record.user_id, record.guild_id);
        return maybe;
      } catch (e) {
        throw e;
      }
    }

    // handle duplicate-key error code 23505 from Postgres if present (caught at exception level)
    if (err && (err.code === '23505' || emsg.includes('duplicate key') || emsg.includes('violates unique constraint'))) {
      try {
        const conflictQuery = await supabase.from('comp_verifications')
          .select()
          .eq('guild_id', record.guild_id)
          .eq('player_tag', record.player_tag)
          .order('created_at', { ascending: false })
          .limit(1);
        const conflictRecord = (!conflictQuery.error && conflictQuery.data && conflictQuery.data.length > 0) ? conflictQuery.data[0] : null;

        const suffix = `__dup__${crypto.randomUUID().slice(0,8)}`;
        const alt = { ...record };
        alt.player_tag = (alt.player_tag ? String(alt.player_tag).slice(0, 120) : 'unknown') + suffix;
        alt.flagged = true;
        alt.flag_reason = `Duplicate tag conflict during insert; original tag: ${record.player_tag}`;
        if (alt.raw_openai === undefined) delete alt.raw_openai;
        if (alt.source === undefined) delete alt.source;
        delete alt.raw_ocr;

        const altResp = await supabase.from('comp_verifications').insert([alt]).select().maybeSingle();
        if (!altResp.error && altResp.data) {
          try {
            const guild = await client.guilds.fetch(record.guild_id).catch(() => null);
            if (guild) {
              await logToGuild(guild, 'Duplicate-key fallback saved', `A new submission for tag **${record.player_tag}** conflicted with an existing record. The submission was saved as **${alt.player_tag}** and flagged for admin review.`);
            }
          } catch (_) {}

          // --- NEW: create pending approval and DM admin for this fallback insertion (same as above) ---
          try {
            const reqId = crypto.randomUUID();
            const pending = {
              userId: record.user_id,
              guildId: record.guild_id,
              prevTag: conflictRecord ? conflictRecord.player_tag : null,
              newTag: record.player_tag,
              newPlatform: record.platform || null,
              oldImage: conflictRecord ? conflictRecord.image_url : null,
              newImage: record.image_url || null,
              otherUserId: conflictRecord ? conflictRecord.user_id : null,
              otherImage: conflictRecord ? conflictRecord.image_url : null,
              altSavedTag: alt.player_tag
            };
            pendingApprovals.set(reqId, pending);

            const adminUser = await client.users.fetch(ADMIN_USER_ID).catch(() => null);
            const adminEmbed = new EmbedBuilder()
              .setTitle('Duplicate-key fallback saved — admin attention required')
              .setDescription(`A new submission for tag **${record.player_tag}** conflicted with an existing record. The submission was automatically saved as **${alt.player_tag}** and flagged for admin review.\n\nIf you want to make the newly-saved submission the canonical tag, approve it. Otherwise, deny to keep the existing owner.`)
              .addFields(
                { name: 'Guild', value: `<@${record.guild_id}> (${record.guild_id})`, inline: true },
                { name: 'New submitter', value: `<@${record.user_id}>`, inline: true },
                { name: 'Existing owner', value: conflictRecord ? `<@${conflictRecord.user_id}>` : 'None found', inline: true }
              )
              .setTimestamp();

            const comps = new ActionRowBuilder().addComponents(
              new ButtonBuilder().setCustomId(`admin_approve:${reqId}`).setLabel('Approve new submission').setStyle(ButtonStyle.Success),
              new ButtonBuilder().setCustomId(`admin_deny:${reqId}`).setLabel('Keep existing / Deny new').setStyle(ButtonStyle.Danger)
            );

            if (adminUser) {
              try {
                await adminUser.send({ embeds: [adminEmbed], components: [comps] }).catch(() => { throw new Error('admin DM send failed'); });
                if (pending.oldImage) await adminUser.send({ content: `Existing owner image for <@${conflictRecord.user_id}>: ${pending.oldImage}` }).catch(() => null);
                if (pending.newImage) await adminUser.send({ content: `New submitter image: ${pending.newImage}` }).catch(() => null);
              } catch (e) {
                if (OCR_DEBUG) console.warn('Failed to DM admin for duplicate-key fallback:', e?.message || e);
                try { if (record.guild_id) { const g = await client.guilds.fetch(record.guild_id).catch(() => null); if (g) await logToGuild(g, 'Duplicate-key fallback - admin DM failed', `Could not DM admin for duplicate-key fallback for tag ${record.player_tag}`); } } catch (_) {}
              }
            } else {
              if (OCR_DEBUG) console.warn('Admin user not found for duplicate-key fallback', ADMIN_USER_ID);
            }
          } catch (e) {
            if (OCR_DEBUG) console.warn('Failed creating pending approval for duplicate-key fallback:', e?.message || e);
          }

          return altResp.data;
        } else {
          throw err;
        }
      } catch (e) {
        throw err;
      }
    }

    // if all else fails, rethrow
    throw err;
  }
}

async function updateLatestRecord(user_id, guild_id, updates) {
  // Update only latest row for the user/guild
  // Supabase supports using .order() + .limit() on select, but for update we emulate by selecting the id first.
  try {
    const sel = await supabase.from('comp_verifications')
      .select('id')
      .eq('user_id', user_id)
      .eq('guild_id', guild_id)
      .order('created_at', { ascending: false })
      .limit(1)
      .maybeSingle();

    if (sel.error) throw sel.error;
    const row = sel.data;
    if (!row || !row.id) {
      if (OCR_DEBUG) console.warn('updateLatestRecord: no latest row to update for', user_id, guild_id);
      return null;
    }
    const updated = await supabase.from('comp_verifications').update(updates).eq('id', row.id).select().maybeSingle();
    if (updated.error) throw updated.error;
    return updated.data || null;
  } catch (err) {
    throw err;
  }
}

async function getLatestRecord(user_id, guild_id) {
  try {
    const { data, error } = await supabase.from('comp_verifications')
      .select()
      .eq('user_id', user_id)
      .eq('guild_id', guild_id)
      .order('created_at', { ascending: false })
      .limit(1);
    if (error) throw error;
    return (data && data[0]) || null;
  } catch (err) {
    throw err;
  }
}

async function findRecordByHash(user_id, guild_id, image_hash) {
  try {
    const { data, error } = await supabase.from('comp_verifications').select().eq('user_id', user_id).eq('guild_id', guild_id).eq('image_hash', image_hash);
    if (error) throw error;
    return (data && data[0]) || null;
  } catch (err) {
    if (OCR_DEBUG) console.warn('findRecordByHash error', err?.message || err);
    return null;
  }
}

async function findRecordByTag(guild_id, tag) {
  try {
    if (!tag) return null;
    const { data, error } = await supabase.from('comp_verifications')
      .select()
      .eq('guild_id', guild_id)
      .eq('player_tag', tag)
      .order('created_at', { ascending: false })
      .limit(1)
      .maybeSingle();
    if (error) throw error;
    return data || null;
  } catch (err) {
    if (OCR_DEBUG) console.warn('findRecordByTag error', err?.message || err);
    return null;
  }
}

// evaluation helpers
function evaluateStats({ win_pct, games_played }) {
  // Coerce into numbers and validate
  const gp = Number(games_played);
  const wp = Number(win_pct);

  const meetsGames = Number.isFinite(gp) && gp >= MIN_GAMES;
  const meetsWin = Number.isFinite(wp) && wp >= MIN_WIN_PCT;
  const passed = meetsGames && meetsWin;
  return { passed, meetsGames, meetsWin };
}

// role helpers
async function ensureCompSettingsRow(guildId) {
  try {
    const upsertObj = {
      guild_id: guildId,
      channel_name: CHANNEL_NAME,
      min_games: MIN_GAMES,
      min_win_pct: MIN_WIN_PCT,
      reverify_days: REVERIFY_DAYS,
      updated_at: new Date().toISOString()
    };
    await supabase.from('comp_settings').upsert(upsertObj, { onConflict: ['guild_id'] });
    const sel = await supabase.from('comp_settings').select('*').eq('guild_id', guildId).maybeSingle();
    return sel.data;
  } catch (err) {
    console.warn('ensureCompSettingsRow error:', err?.message || err);
    return null;
  }
}

async function ensureRoleForGuild(guild) {
  try {
    await ensureCompSettingsRow(guild.id);

    const sel = await supabase.from('comp_settings').select('role_id').eq('guild_id', guild.id).maybeSingle();
    if (sel.error) console.warn('comp_settings select error:', sel.error.message || sel.error);
    let roleId = sel.data?.role_id || ROLE_ID_ENV || null;

    if (roleId) {
      const fetched = await guild.roles.fetch(roleId).catch(() => null);
      if (fetched) {
        await supabase.from('comp_settings').upsert({ guild_id: guild.id, role_id: fetched.id }, { onConflict: ['guild_id'] });
        return fetched;
      } else {
        if (OCR_DEBUG) console.log('Persisted role_id not found in guild, will create a new role.');
        roleId = null;
        await supabase.from('comp_settings').upsert({ guild_id: guild.id, role_id: null }, { onConflict: ['guild_id'] });
      }
    }

    const found = guild.roles.cache.find(r => r.name === 'Comp Verified' || r.name === 'Comp');
    if (found) {
      await supabase.from('comp_settings').upsert({ guild_id: guild.id, role_id: found.id }, { onConflict: ['guild_id'] });
      return found;
    }

    const newRole = await guild.roles.create({
      name: 'Comp Verified',
      color: 0x00AE86,
      hoist: false,
      mentionable: false,
      reason: 'Auto-created comp verification role'
    });

    const up2 = await supabase.from('comp_settings').upsert({ guild_id: guild.id, role_id: newRole.id }, { onConflict: ['guild_id'] });
    if (up2.error) console.warn('Failed to persist created role to comp_settings:', up2.error.message || up2.error);

    try {
      const botMember = await guild.members.fetch(client.user.id).catch(() => null);
      if (botMember && botMember.roles && botMember.roles.highest) {
        const botHighestPos = botMember.roles.highest.position;
        if (newRole.position >= botHighestPos) {
          try {
            await newRole.setPosition(Math.max(0, botHighestPos - 1));
            if (OCR_DEBUG) console.log('Adjusted new role position to be below bot highest role.');
          } catch (posErr) {
            if (OCR_DEBUG) console.warn('Could not change new role position:', posErr?.message || posErr);
          }
        }
      }
    } catch (e) {
      if (OCR_DEBUG) console.warn('Error while attempting to adjust role position:', e?.message || e);
    }

    console.log(`Created role ${newRole.name} (${newRole.id}) in guild ${guild.id}`);
    return newRole;
  } catch (err) {
    console.error('ensureRoleForGuild error:', err?.message || err);
    return null;
  }
}

async function getRoleIdForGuild(guildId) {
  try {
    const { data, error } = await supabase.from('comp_settings').select('role_id').eq('guild_id', guildId).maybeSingle();
    if (!error && data?.role_id) return data.role_id;
  } catch (_) {}
  // fallback order: explicit env ROLE_ID -> provided fallback constant -> null
  if (ROLE_ID_ENV) return ROLE_ID_ENV;
  if (FALLBACK_ROLE_ID) return FALLBACK_ROLE_ID;
  return null;
}

// Helper: post a player card embed to the configured player card channel
async function postPlayerCardToChannel(guild, record) {
  try {
    if (!record) return;
    // Try guild scoped fetch first, then global fetch
    let ch = null;
    if (guild && guild.channels) {
      try { ch = await guild.channels.fetch(PLAYER_CARD_CHANNEL_ID).catch(() => null); } catch (_) { ch = null; }
    }
    if (!ch) {
      try { ch = await client.channels.fetch(PLAYER_CARD_CHANNEL_ID).catch(() => null); } catch (_) { ch = null; }
    }
    if (!ch || typeof ch.send !== 'function') {
      if (OCR_DEBUG) console.warn('Player card channel not found or not sendable:', PLAYER_CARD_CHANNEL_ID);
      return;
    }

    // Resolve display name
    let displayName = record.username || `Player ${record.user_id}`;
    try {
      if (guild) {
        const member = await guild.members.fetch(record.user_id).catch(() => null);
        if (member) displayName = member.displayName || member.user?.username || displayName;
      } else {
        const u = await client.users.fetch(record.user_id).catch(() => null);
        if (u) displayName = u.username || displayName;
      }
    } catch (_) {}

    const embed = new EmbedBuilder()
      .setTitle(`${displayName} — Comp Stats`)
      .addFields(
        { name: 'Win percentage', value: record.win_pct != null ? String(record.win_pct) : 'N/A', inline: true },
        { name: 'Games played', value: record.games_played != null ? String(record.games_played) : 'N/A', inline: true },
        { name: 'Points', value: record.points != null ? String(record.points) : 'N/A', inline: true },
        { name: 'Platform / Tag', value: `${record.platform ?? 'N/A'}${record.player_tag ? ` / ${record.player_tag}` : ''}`, inline: false },
        { name: 'Verified', value: record.verified ? `Yes — ${record.verified_at ? new Date(record.verified_at).toLocaleString() : 'N/A'}` : 'No', inline: false },
        { name: 'Flagged', value: record.flagged ? `Yes — ${record.flag_reason ?? 'Needs review'}` : 'No', inline: false }
      )
      .setTimestamp();

    if (record.image_url) embed.setImage(record.image_url);

    await ch.send({ embeds: [embed] }).catch(e => {
      if (OCR_DEBUG) console.warn('Failed to send player card embed:', e?.message || e);
    });
  } catch (err) {
    if (OCR_DEBUG) console.warn('postPlayerCardToChannel error:', err?.message || err);
  }
}

// ================ Ready handler =================
async function onReadyHandler() {
  if (readyRan) {
    console.log('Ready handler already ran; skipping duplicate invocation.');
    return;
  }
  readyRan = true;

  console.log('Logged in as', client.user.tag);
  try { await registerCommands(); } catch (e) { console.warn('Register commands error:', e?.message || e); }

  if (openai) {
    activeOpenAIModel = await chooseWorkingOpenAIModel(PREFERRED_OPENAI_MODEL);
    if (!activeOpenAIModel) {
      console.warn('No OpenAI model candidates responded successfully. Image parsing disabled until a working model/API key is available.');
    }
  } else {
    console.warn('OpenAI client not initialized; image parsing disabled.');
  }

  try {
    const testGuild = await client.guilds.fetch(TEST_GUILD_ID).catch(() => null);
    const prodGuild = GUILD_ID ? await client.guilds.fetch(GUILD_ID).catch(() => null) : null;

    if (testGuild) {
      await ensureCompSettingsRow(testGuild.id);
      await findOrCreateCategory(testGuild).catch(() => null);
      const ch = await findOrCreateChannel(testGuild);
      if (ch) await postOrUpdateVerificationEmbed(ch);
      await ensureRoleForGuild(testGuild);
      await findOrCreateLogChannel(testGuild).catch(() => null);
    }
    if (prodGuild && prodGuild.id !== testGuild?.id) {
      await ensureCompSettingsRow(prodGuild.id);
      await findOrCreateCategory(prodGuild).catch(() => null);
      const ch = await findOrCreateChannel(prodGuild);
      if (ch) await postOrUpdateVerificationEmbed(ch);
      await ensureRoleForGuild(prodGuild);
      await findOrCreateLogChannel(prodGuild).catch(() => null);
    }
  } catch (err) {
    console.warn('Error ensuring channels/roles:', err?.message || err);
  }

  cron.schedule('0 12 * * *', async () => {
    console.log('Running daily reverify check...');
    try {
      const { data, error } = await supabase.from('comp_verifications').select().eq('guild_id', GUILD_ID).eq('verified', true);
      if (error) {
        console.warn('Supabase fetch error for daily check:', error.message);
        return;
      }
      for (const rec of data || []) {
        if (!rec.verified_at) continue;
        const verifiedAt = new Date(rec.verified_at);
        const ageDays = (Date.now() - verifiedAt.getTime()) / (1000 * 60 * 60 * 24);
        if (ageDays >= REVERIFY_DAYS) {
          try {
            const user = await client.users.fetch(rec.user_id);
            await user.send(`Hi — your Comp verification has expired (more than ${REVERIFY_DAYS} days). Please re-verify by clicking the Verify button in the "${CHANNEL_NAME}" channel on the server.`);
            try {
              const guild = await client.guilds.fetch(rec.guild_id).catch(() => null);
              if (guild) {
                await logToGuild(guild, 'Verification expired', `User <@${rec.user_id}>'s verification expired after ${REVERIFY_DAYS} days.`);
              }
            } catch (_) {}
          } catch (err) {
            console.warn('Could not DM user for reverify:', err.message);
          }
        }
      }
    } catch (err) {
      console.warn('Error in daily reverify cron:', err.message);
    }
  }, { timezone: 'UTC' });
}

client.once('ready', onReadyHandler);
client.once('clientReady', onReadyHandler);

// ================ embed posting =================
async function fetchPinsAndFindExisting(channel) {
  const pins = await fetchPinsSafe(channel);
  return Array.isArray(pins) ? pins : [];
}

async function postOrUpdateVerificationEmbed(channel) {
  // Build a clearer, up-to-date description that documents the current bot behavior.
  // Uses runtime config values (MIN_WIN_PCT, MIN_GAMES, REVERIFY_DAYS) so the embed stays accurate.
  const description =
    'This bot verifies Competitive (Comp) players for access to Comp channels.\n\n' +
    'How it works: Click the **Verify** button to receive a DM with instructions. Upload a single clear screenshot of your NBA2K Stats screen in the DM. Make sure the **Games Played** number and **Win percentage** are visible. The bot will analyze the screenshot and let you know if you meet the requirements.\n\n' +
    `Automatic verification requirements: **Win percentage** must be at least **${MIN_WIN_PCT}%** and **Games Played** must be at least **${MIN_GAMES}**.\n` +
    '- If you have **fewer than the minimum games**, your profile will still be saved in the system (you can link your account), but you **will not** receive the Comp role until you meet both thresholds.\n' +
    `- Verifications expire every **${REVERIFY_DAYS} days**; you must re-verify after that period to keep the Comp role.\n\n` +
    'Duplicate / conflict handling: If a player tag already exists in the system, the new submission will be **saved and flagged** for admin review. Admins will be notified and will decide which submission to keep. If the bot detects a conflict it cannot resolve automatically, it will preserve both submissions and request manual admin action.\n\n' +
    'What Comp players get: Verified Comp players are granted a role which provides access to Comp channels and related areas on the server.\n\n' +
    'Commands: Use the **/player view** command to look up a player\'s saved stats. This will show the server nickname when available.\n\n' +
    'If you need help or notice an issue (for example an incorrectly flagged submission), contact a server administrator.';

  const embed = new EmbedBuilder()
    .setTitle('Comp Verification - NBA2K26')
    .setDescription(description)
    .setColor(0x1ABC9C)
    .setFooter({ text: 'Comp Verification Bot - Grants access to Comp channels for verified players' })
    .setTimestamp();

  const verifyRow = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId('start_verify').setLabel('Verify').setStyle(ButtonStyle.Primary)
  );

  try {
    const pinArr = await fetchPinsAndFindExisting(channel);
    const existing = pinArr.find(m => m.author?.id === client.user.id && m.embeds?.length && m.embeds[0]?.title === 'Comp Verification - NBA2K26');

    if (existing) {
      await existing.edit({ embeds: [embed], components: [verifyRow] });
      console.log('Updated existing pinned verification embed in', channel.name);
      return existing;
    } else {
      const sent = await channel.send({ embeds: [embed], components: [verifyRow] });
      try { await sent.pin(); } catch (e) { if (OCR_DEBUG) console.warn('Pin failed', e?.message || e); }
      console.log('Posted and pinned verification embed in', channel.name);
      return sent;
    }
  } catch (err) {
    console.warn('Could not post/update embed in channel:', err?.message || err);
    try {
      const sent = await channel.send({ embeds: [embed], components: [verifyRow] });
      try { await sent.pin(); } catch (e) {}
      return sent;
    } catch (e) {
      console.warn('Fallback send also failed:', e?.message || e);
      return null;
    }
  }
}

// ================ find guild utility ================
async function findGuildForUser(userId) {
  const candidates = [];
  for (const [id, guild] of client.guilds.cache) {
    try {
      const member = await guild.members.fetch(userId).catch(() => null);
      if (member) candidates.push({ guild, member });
    } catch (err) {
      if (OCR_DEBUG) console.warn('Error fetching member in guild', guild.id, err?.message || err);
    }
  }
  if (candidates.length === 0) return null;
  if (TEST_GUILD_ID) {
    const t = candidates.find(c => c.guild.id === TEST_GUILD_ID);
    if (t) return t;
  }
  if (GUILD_ID) {
    const p = candidates.find(c => c.guild.id === GUILD_ID);
    if (p) return p;
  }
  return candidates[0];
}

// ================ interactions (verify button + admin approval) ================
client.on('interactionCreate', async (interaction) => {
  try {
    if (interaction.isButton()) {
      // Start verify button
      if (interaction.customId === 'start_verify') {
        // Use flags number 64 for ephemeral
        await interaction.reply({ content: 'I sent you a DM with verification instructions. Check your DMs.', flags: 64 });
        try {
          const dm = await interaction.user.createDM();
          await dm.send('Please upload a single clear screenshot of your NBA2K Stats screen in this DM. Make sure Games Played and Win percentage are visible. After upload, I will process it and notify you in DM.');
        } catch (err) {
          await interaction.followUp({ content: 'I could not DM you. Please enable DMs from server members or message the bot directly.', flags: 64 });
        }
      }

      // admin approval buttons
      if (interaction.customId && (interaction.customId.startsWith('admin_approve:') || interaction.customId.startsWith('admin_deny:'))) {
        // only admin may click
        if (String(interaction.user.id) !== String(ADMIN_USER_ID)) {
          await interaction.reply({ content: 'You are not authorized to perform this action.', flags: 64 });
          return;
        }

        const parts = interaction.customId.split(':');
        const action = parts[0]; // admin_approve or admin_deny
        const reqId = parts[1];
        const pending = pendingApprovals.get(reqId);
        if (!pending) {
          await interaction.reply({ content: 'This approval request is no longer valid or was already handled.', flags: 64 });
          return;
        }

        // Approve
        if (action === 'admin_approve') {
          try {
            // --- NEW: if another existing record already has the desired tag, move that record aside first ---
            if (pending.newTag) {
              try {
                const conflictQuery = await supabase.from('comp_verifications')
                  .select()
                  .eq('guild_id', pending.guildId)
                  .eq('player_tag', pending.newTag)
                  .neq('user_id', pending.userId)
                  .order('created_at', { ascending: false })
                  .limit(1);

                const conflicting = (!conflictQuery.error && conflictQuery.data && conflictQuery.data.length > 0) ? conflictQuery.data[0] : null;
                if (conflicting) {
                  // rename the existing owner's tag so we can assign it to the new user
                  const newTagForOld = `${conflicting.player_tag}__reassigned__${crypto.randomUUID().slice(0,8)}`;
                  await updateLatestRecord(conflicting.user_id, pending.guildId, {
                    player_tag: newTagForOld,
                    flagged: true,
                    flag_reason: `Tag reassigned to <@${pending.userId}> by admin`
                  }).catch(e => { if (OCR_DEBUG) console.warn('Failed to move aside conflicting record:', e?.message || e); });

                  try {
                    // notify old owner
                    const oldUser = await client.users.fetch(conflicting.user_id).catch(() => null);
                    if (oldUser) {
                      await oldUser.send(`An admin reassigned your player tag **${conflicting.player_tag}** to another account as part of a dispute resolution. Your saved tag has been renamed to **${newTagForOld}** and flagged for admin review. If this is unexpected, contact an admin.`).catch(() => null);
                    }
                    const g = await client.guilds.fetch(pending.guildId).catch(() => null);
                    if (g) await logToGuild(g, 'Tag reassigned by admin', `Existing owner <@${conflicting.user_id}>'s tag ${conflicting.player_tag} was reassigned to allow assignment to <@${pending.userId}> by admin.`);
                  } catch (_) {}
                }
              } catch (e) {
                if (OCR_DEBUG) console.warn('Error checking/moving aside pre-existing tag during admin approval:', e?.message || e);
              }
            }

            // Update latest DB record with new tag/platform and clear flagged state on pending user
            await updateLatestRecord(pending.userId, pending.guildId, {
              player_tag: pending.newTag,
              platform: pending.newPlatform,
              flagged: false,
              flag_reason: null,
              image_url: pending.newImage || null
            });

            // If an existing other user was flagged due to this duplicate, clear their flagged state if admin wants (optional)
            if (pending.otherUserId) {
              try {
                // clear flagged state for the other user's latest record
                await updateLatestRecord(pending.otherUserId, pending.guildId, { flagged: false, flag_reason: null });
              } catch (e) {
                if (OCR_DEBUG) console.warn('Failed to clear flagged state for other user:', e?.message || e);
              }
            }

            // After DB update, re-fetch latest record and attempt to give role if stats meet thresholds
            const latest = await getLatestRecord(pending.userId, pending.guildId).catch(() => null);
            const guild = await client.guilds.fetch(pending.guildId).catch(() => null);
            const u = await client.users.fetch(pending.userId).catch(() => null);

            if (latest && guild) {
              const evalRes = evaluateStats({ win_pct: latest.win_pct, games_played: latest.games_played });

              if (evalRes.passed) {
                // try add role automatically
                try {
                  // ensure role object
                  let roleObj = null;
                  const persistedRoleId = await getRoleIdForGuild(guild.id);
                  if (persistedRoleId) {
                    roleObj = await guild.roles.fetch(persistedRoleId).catch(() => null);
                    if (!roleObj) roleObj = await ensureRoleForGuild(guild);
                  } else {
                    roleObj = await ensureRoleForGuild(guild);
                  }

                  if (roleObj) {
                    const member = await guild.members.fetch(pending.userId).catch(() => null);
                    const botMember = await guild.members.fetch(client.user.id).catch(() => null);
                    const botCanManageRoles = botMember ? botMember.permissions.has(PermissionsBitField.Flags.ManageRoles) : false;
                    const botHighestPos = botMember ? botMember.roles.highest.position : -1;

                    if (!member) {
                      if (u) await u.send('Admin approved your tag change but I could not find you in the guild to add the Comp role. Please rejoin or contact an admin.');
                      await logToGuild(guild, 'Admin approval - member not found', `Approved tag change for <@${pending.userId}> but member not present to grant role.`);
                    } else if (!botCanManageRoles) {
                      if (u) await u.send('Admin approved your tag change. You meet verification thresholds but the bot lacks Manage Roles permission to add your Comp role. Ask an admin to assign it.');
                      await logToGuild(guild, 'Admin approval - missing ManageRoles', `Approved tag change for <@${pending.userId}> but bot lacks ManageRoles.`);
                    } else if (roleObj.position >= botHighestPos) {
                      if (u) await u.send('Admin approved your tag change. You meet verification thresholds but the bot role is lower than the verification role. Ask an admin to move the bot role above the verification role.');
                      await logToGuild(guild, 'Admin approval - hierarchy issue', `Approved tag change for <@${pending.userId}> but bot role lower than verification role.`);
                    } else {
                      // add role and update DB verified
                      await member.roles.add(roleObj.id, 'Admin-approved player tag change + meets verification thresholds');
                      await updateLatestRecord(pending.userId, pending.guildId, {
                        verified: true,
                        verified_at: new Date().toISOString(),
                        expires_at: new Date(Date.now() + REVERIFY_DAYS * 24 * 60 * 60 * 1000).toISOString()
                      });
                      if (u) await u.send(`An admin approved your tag change and you meet the verification thresholds. You have been granted the Comp role.`);
                      await logToGuild(guild, 'Player tag change approved & role granted', `Admin approved tag change and granted role for <@${pending.userId}>. New tag: ${pending.newTag ?? 'N/A'}.`);

                      // Post player card to configured channel
                      try {
                        const latestAfter = await getLatestRecord(pending.userId, pending.guildId);
                        await postPlayerCardToChannel(guild, latestAfter);
                      } catch (e) {
                        if (OCR_DEBUG) console.warn('Failed to post player card after admin approval:', e?.message || e);
                      }
                    }
                  } else {
                    if (u) await u.send('Admin approved your tag change, but I could not find or create the verification role in the server. Please contact a server admin.');
                    if (guild) await logToGuild(guild, 'Admin approval - role missing', `Approved tag change for <@${pending.userId}> but role missing/creation failed.`);
                  }
                } catch (err) {
                  if (u) await u.send('Admin approved your tag change, but the bot failed to add your Comp role due to an error. Contact an admin.');
                  if (guild) await logToGuild(guild, 'Admin approval - role assign error', `Error assigning role to <@${pending.userId}>: ${err?.message || err}`);
                }
              } else {
                // profile updated but stats do not meet threshold
                if (u) await u.send(`An admin approved your player tag change. Your profile was updated (new tag: ${pending.newTag ?? 'N/A'}), but your saved stats do not meet the verification thresholds (Win%: ${latest.win_pct ?? 'N/A'}, Games: ${latest.games_played ?? 'N/A'}). No Comp role was assigned.`);
                if (guild) await logToGuild(guild, 'Player tag change approved - no role (stats low)', `Admin approved tag change for <@${pending.userId}> but stats do not meet thresholds. New tag: ${pending.newTag ?? 'N/A'}.`);
              }
            } else {
              // still notify user/admin
              if (u) await u.send(`An admin approved your tag change. Your player card was updated, but I couldn't finalize role assignment automatically.`);
              if (guild) await logToGuild(guild, 'Player tag change approved - post-update check failed', `Approved tag change for <@${pending.userId}> but post-update checks failed.`);
            }

            await interaction.update({ content: 'Approved — user has been updated.', components: [] });
          } catch (e) {
            console.warn('admin approval error:', e?.message || e);
            await interaction.update({ content: 'Failed to apply approval. Check logs.', components: [] });
          } finally {
            pendingApprovals.delete(reqId);
          }
        } else {
          // Deny
          try {
            await updateLatestRecord(pending.userId, pending.guildId, {
              flagged: true,
              flag_reason: 'Denied by admin'
            });
            const u = await client.users.fetch(pending.userId).catch(() => null);
            if (u) await u.send(`An admin denied your requested player tag change. If you believe this is a mistake, contact an admin.`);
            const g = await client.guilds.fetch(pending.guildId).catch(() => null);
            if (g) await logToGuild(g, 'Player tag change denied', `Admin denied tag change for <@${pending.userId}>. Prev: ${pending.prevTag}, New: ${pending.newTag}.`);
            await interaction.update({ content: 'Denied — user has been notified.', components: [] });
          } catch (e) {
            console.warn('admin deny error:', e?.message || e);
            await interaction.update({ content: 'Failed to apply denial. Check logs.', components: [] });
          } finally {
            pendingApprovals.delete(reqId);
          }
        }
      }
    } else if (interaction.isChatInputCommand()) {
      if (interaction.commandName === 'player') {
        const sub = interaction.options.getSubcommand();
        if (sub === 'view') {
          const targetUser = interaction.options.getUser('user') || interaction.user;
          await interaction.deferReply();

          // Use the current guild where the command was run (if available); otherwise fall back to config
          const guildIdForLookup = interaction.guildId || GUILD_ID || TEST_GUILD_ID;
          let displayName = targetUser.username;
          try {
            // prefer the guild where the command was run (so we show server nickname)
            const guild = await client.guilds.fetch(guildIdForLookup).catch(() => null);
            if (guild) {
              const member = await guild.members.fetch(targetUser.id).catch(() => null);
              // Use member.displayName which is the most accurate "server display name" (nickname or username fallback)
              if (member) displayName = member.displayName || member.user.username || targetUser.username;
            }
          } catch (e) {
            if (OCR_DEBUG) console.warn('Could not fetch member for nickname:', e?.message || e);
          }

          const rec = await getLatestRecord(targetUser.id, guildIdForLookup);
          if (!rec) return interaction.editReply({ content: `No saved verification found for ${targetUser.username}.` });

          const embed = new EmbedBuilder()
            .setTitle(`${displayName} — Comp Stats`)
            .addFields(
              { name: 'Win percentage', value: rec.win_pct != null ? String(rec.win_pct) : 'N/A', inline: true },
              { name: 'Games played', value: rec.games_played != null ? String(rec.games_played) : 'N/A', inline: true },
              { name: 'Points', value: rec.points != null ? String(rec.points) : 'N/A', inline: true },
              { name: 'Platform / Tag', value: `${rec.platform ?? 'N/A'} ${rec.player_tag ? ` / ${rec.player_tag}` : ''}`, inline: false },
              { name: 'Verified', value: rec.verified ? `Yes — ${rec.verified_at ? new Date(rec.verified_at).toLocaleString() : 'N/A'}` : 'No', inline: false },
              { name: 'Flagged', value: rec.flagged ? `Yes — ${rec.flag_reason ?? 'Needs review'}` : 'No', inline: false }
            )
            .setTimestamp();

          if (rec.image_url) embed.setImage(rec.image_url);
          return interaction.editReply({ embeds: [embed] });
        }
      }
    }
  } catch (err) {
    console.warn('Interaction error:', err?.message || err);
  }
});

// ================ DM handler =================
client.on('messageCreate', async (message) => {
  if (message.author.bot) return;
  if (message.channel.type !== ChannelType.DM) return;

  if (!message.attachments || message.attachments.size === 0) {
    await message.reply('Please attach an image screenshot showing your Stats screen (Games Played and Win percentage).');
    return;
  }

  const attachment = message.attachments.first();
  const url = attachment.url;

  if (OCR_DEBUG) {
    console.log('Received DM attachment:', { url, contentType: attachment.contentType, size: attachment.size, height: attachment.height, width: attachment.width });
  }

  await message.reply('Thanks — processing your screenshot now.');

  try {
    if (!openai || !activeOpenAIModel) {
      await message.author.send('Image parsing is currently unavailable because the OpenAI API or a working model is not configured. Please contact a server admin to set a working OPENAI_API_KEY and model.');
      return;
    }

    // download to compute hash
    const buffer = await downloadImageToBuffer(url);
    const image_hash = await computeImageHash(buffer);

    // find guild & member context
    const mutual = await findGuildForUser(message.author.id);
    if (!mutual) {
      await message.author.send('I could not find a server where you and this bot are both present. Make sure you joined the server you want verification for and try again.');
      return;
    }
    const guild = mutual.guild;
    const member = mutual.member;
    const targetGuildId = guild.id;

    // IMPORTANT: fetch the previous (latest) record BEFORE inserting the new one.
    const prevRec = await getLatestRecord(message.author.id, targetGuildId).catch(() => null);

    // Rate limit: only allow 1 screenshot per hour
    if (prevRec && prevRec.created_at) {
      const lastMs = new Date(prevRec.created_at).getTime();
      const elapsedMs = Date.now() - lastMs;
      const hourMs = 60 * 60 * 1000;
      if (elapsedMs < hourMs) {
        const minutesLeft = Math.ceil((hourMs - elapsedMs) / 60000);
        await message.author.send(`Please wait ${minutesLeft} minute(s) before uploading another screenshot. Only one upload per hour is allowed.`);
        if (OCR_DEBUG) await logToGuild(guild, 'Rate limit blocked', `User <@${message.author.id}> tried to upload within rate limit (${minutesLeft}m left).`);
        return;
      }
    }

    // duplicate image detection (exact same image uploaded before by same user)
    const dup = await findRecordByHash(message.author.id, targetGuildId, image_hash);
    if (dup) {
      await message.author.send('This screenshot has already been uploaded by you previously. Duplicate screenshots are not allowed. If you believe this is a mistake, contact an admin.');
      await logToGuild(guild, 'Duplicate screenshot blocked', `User <@${message.author.id}> attempted to upload a duplicate screenshot (hash ${image_hash}).`);
      return;
    }

    // parse image (OpenAI)
    const parsed = await parseImageStats(url);
    parsed.image_hash = parsed.image_hash || image_hash;

    // --- Defensive normalization: coerce & validate parsed fields so empty/whitespace tags are treated as missing ---
    parsed.player_tag = parsed.player_tag ? String(parsed.player_tag).trim() : null;
    parsed.games_played = (parsed.games_played != null && parsed.games_played !== '') ? Number(parsed.games_played) : null;
    parsed.win_pct = (parsed.win_pct != null && parsed.win_pct !== '') ? Number(parsed.win_pct) : null;
    if (!Number.isFinite(parsed.games_played)) parsed.games_played = null;
    if (!Number.isFinite(parsed.win_pct)) parsed.win_pct = null;

    // If we could not parse the essential fields, refuse and do NOT save
    const essentialMissing = (parsed.player_tag == null) || (parsed.games_played == null) || (parsed.win_pct == null);
    if (essentialMissing) {
      await message.author.send('I could not reliably read important parts of your screenshot (games, win%, or player tag). Please upload a clear full screenshot where your player tag is visible and re-try. Do not crop the tag. Your upload was not saved.');
      await logToGuild(guild, 'Unreadable screenshot refused', `User <@${message.author.id}> uploaded an unreadable screenshot. Parsed: ${JSON.stringify({ player_tag: parsed.player_tag, games_played: parsed.games_played, win_pct: parsed.win_pct })}`);
      return;
    }

    // Check for duplicate player_tag across the guild (other users)
    let conflictRecord = null;
    try {
      // search for any record with same tag but different user
      const q = await supabase.from('comp_verifications')
        .select()
        .eq('guild_id', targetGuildId)
        .eq('player_tag', parsed.player_tag)
        .neq('user_id', message.author.id)
        .order('created_at', { ascending: false })
        .limit(1);
      if (!q.error && q.data && q.data.length > 0) conflictRecord = q.data[0];
    } catch (e) {
      if (OCR_DEBUG) console.warn('Error checking duplicate tag:', e?.message || e);
    }

    // If duplicate tag exists: save as flagged and alert admin with both screenshots
    if (conflictRecord) {
      const expiresAt = new Date(Date.now() + REVERIFY_DAYS * 24 * 60 * 60 * 1000).toISOString();
      const toInsert = {
        user_id: message.author.id,
        username: `${message.author.username}#${message.author.discriminator}`,
        guild_id: targetGuildId,
        win_pct: parsed.win_pct,
        games_played: parsed.games_played,
        points: parsed.points || null,
        rebounds: parsed.rebounds || null,
        assists: parsed.assists || null,
        player_tag: parsed.player_tag || null,
        platform: parsed.platform || null,
        image_url: url,
        image_hash: parsed.image_hash || image_hash,
        verified: false,
        verified_at: null,
        expires_at: expiresAt,
        flagged: true,
        flag_reason: `Duplicate tag with <@${conflictRecord.user_id}>`,
        created_at: new Date().toISOString()
      };
      if (parsed.raw_openai) toInsert.raw_openai = parsed.raw_openai;
      if (parsed.source) toInsert.source = parsed.source;

      const savedRow = await saveVerificationRecord(toInsert);

      // Optionally flag the existing conflicting record so admin sees it in flagged list too
      try {
        await updateLatestRecord(conflictRecord.user_id, targetGuildId, { flagged: true, flag_reason: `Duplicate tag with <@${message.author.id}>` });
      } catch (e) {
        if (OCR_DEBUG) console.warn('Failed to flag existing conflicting record:', e?.message || e);
      }

      // Build admin DM showing both screenshots
      const reqId = crypto.randomUUID();
      const pending = {
        userId: message.author.id,
        guildId: targetGuildId,
        prevTag: conflictRecord.player_tag,
        newTag: parsed.player_tag,
        newPlatform: parsed.platform,
        oldImage: conflictRecord.image_url || null,
        newImage: url,
        otherUserId: conflictRecord.user_id,
        otherImage: conflictRecord.image_url || null
      };
      pendingApprovals.set(reqId, pending);

      const adminUser = await client.users.fetch(ADMIN_USER_ID).catch(() => null);
      const adminEmbed = new EmbedBuilder()
        .setTitle('Duplicate player tag detected')
        .setDescription(`A new submission uses a player tag that already exists in the system.\n\nTag: **${parsed.player_tag}**\nNew submitter: <@${message.author.id}>\nExisting owner: <@${conflictRecord.user_id}>\n\nPlease review the two screenshots below and decide which submission to accept.`)
        .addFields(
          { name: 'Guild', value: `<@${targetGuildId}> (${targetGuildId})`, inline: true },
          { name: 'New submitter', value: `<@${message.author.id}>`, inline: true },
          { name: 'Existing owner', value: `<@${conflictRecord.user_id}>`, inline: true }
        )
        .setTimestamp();

      const comps = new ActionRowBuilder().addComponents(
        new ButtonBuilder().setCustomId(`admin_approve:${reqId}`).setLabel('Approve new submission').setStyle(ButtonStyle.Success),
        new ButtonBuilder().setCustomId(`admin_deny:${reqId}`).setLabel('Keep existing / Deny new').setStyle(ButtonStyle.Danger)
      );

      if (adminUser) {
        try {
          await adminUser.send({ embeds: [adminEmbed], components: [comps] });
          if (pending.oldImage) await adminUser.send({ content: `Existing owner image for <@${conflictRecord.user_id}>: ${pending.oldImage}` });
          if (pending.newImage) await adminUser.send({ content: `New submitter image: ${pending.newImage}` });
          await message.author.send('Your submission was saved but flagged because that player tag already exists. An admin will review this and notify both parties.');
          await client.users.send(conflictRecord.user_id, `Your player tag (${parsed.player_tag}) was used in a new submission and has been flagged. An admin will review the two screenshots.`).catch(() => null);
          await logToGuild(guild, 'Duplicate tag flagged', `User <@${message.author.id}> submitted tag ${parsed.player_tag} which conflicts with <@${conflictRecord.user_id}>.`);
        } catch (e) {
          if (OCR_DEBUG) console.warn('Failed to send DM to admin:', e?.message || e);
          await message.author.send('Could not contact the admin at this time. Please contact a server admin directly.');
          await logToGuild(guild, 'Duplicate tag - admin DM failed', `Could not send DM to admin for duplicate tag ${parsed.player_tag} by <@${message.author.id}>`);
        }
      } else {
        if (OCR_DEBUG) console.warn('Admin user not found', ADMIN_USER_ID);
        await message.author.send('Admin not reachable. Please contact a server admin directly.');
        await logToGuild(guild, 'Duplicate tag - admin not found', `Admin ${ADMIN_USER_ID} not found for duplicate tag ${parsed.player_tag}`);
      }

      return;
    }

    // No duplicate tag conflict -> proceed to save and evaluate as before
    const expiresAt = new Date(Date.now() + REVERIFY_DAYS * 24 * 60 * 60 * 1000).toISOString();
    const toInsert = {
      user_id: message.author.id,
      username: `${message.author.username}#${message.author.discriminator}`,
      guild_id: targetGuildId,
      win_pct: parsed.win_pct,
      games_played: parsed.games_played,
      points: parsed.points || null,
      rebounds: parsed.rebounds || null,
      assists: parsed.assists || null,
      player_tag: parsed.player_tag || null,
      platform: parsed.platform || null,
      image_url: url,
      image_hash: parsed.image_hash || image_hash,
      verified: false,
      verified_at: null,
      expires_at: expiresAt,
      flagged: false,
      flag_reason: null,
      created_at: new Date().toISOString()
    };
    if (parsed.raw_openai) toInsert.raw_openai = parsed.raw_openai;
    if (parsed.source) toInsert.source = parsed.source;

    const savedRow = await saveVerificationRecord(toInsert);

    // Evaluate stats
    const evalRes = evaluateStats({ win_pct: parsed.win_pct, games_played: parsed.games_played });

    // If prev verified and now fails: warn user and attempt removal (existing logic, unchanged)
    if (prevRec && prevRec.verified && (!evalRes.meetsGames || !evalRes.meetsWin)) {
      try {
        const persistedRoleId = await getRoleIdForGuild(guild.id);
        let roleToRemove = null;
        if (persistedRoleId) roleToRemove = await guild.roles.fetch(persistedRoleId).catch(() => null);
        if (!roleToRemove) roleToRemove = guild.roles.cache.find(r => r.name === 'Comp Verified' || r.name === 'Comp') || null;

        if (roleToRemove && member) {
          const botMember = await guild.members.fetch(client.user.id).catch(() => null);
          const botCanManageRoles = botMember ? botMember.permissions.has(PermissionsBitField.Flags.ManageRoles) : false;
          const botHighestPos = botMember ? botMember.roles.highest.position : -1;

          // refresh roles cache and check if the user actually has the role
          try { await member.roles.fetch(); } catch (_) {}

          // Heads-up DM BEFORE removing the role (only if they currently have it)
          try {
            if (member.roles.cache.has(roleToRemove.id)) {
              await message.author.send(
                `Heads up — your new screenshot does not meet the Comp requirements ` +
                `(Win: ${parsed.win_pct ?? 'N/A'}, Games: ${parsed.games_played ?? 'N/A'}). I will attempt to remove your Comp role.`
              );
            }
          } catch (_) {}

          if (!member.roles.cache.has(roleToRemove.id)) {
            await logToGuild(guild, 'Comp role removal skipped', `User <@${message.author.id}> did not have the Comp role (nothing to remove).`);
          } else if (botCanManageRoles && roleToRemove.position < botHighestPos) {
            await member.roles.remove(roleToRemove.id, 'Comp verification revoked: new screenshot does not meet requirements');
            await logToGuild(guild, 'Comp role removed', `User <@${message.author.id}>'s Comp role removed because new screenshot failed requirements. Detected win%: ${parsed.win_pct ?? 'N/A'}, games: ${parsed.games_played ?? 'N/A'}.`);
            try {
              await message.author.send(`Thanks for linking your account — your profile has been saved. You didn't meet the Comp requirements (Win%: ${parsed.win_pct ?? 'N/A'}, Games: ${parsed.games_played ?? 'N/A'}). You can still view other players' stats on the server. Thank you for linking your account.`);
            } catch (e) {}
          } else {
            await message.author.send(`Your new screenshot does not meet the verification requirements (Win: ${parsed.win_pct ?? 'N/A'} Games: ${parsed.games_played ?? 'N/A'}). The bot could not remove the Comp role automatically due to permissions or role order. Please contact a server admin to resolve this. You can still view other players' stats on the server. Thank you for linking your account.`);
            await logToGuild(guild, 'Comp role removal blocked', `User <@${message.author.id}> should have role removed (failed re-check), but bot lacks permission/hierarchy to remove it.`);
          }
        } else {
          await logToGuild(guild, 'Comp role removal skipped', `User <@${message.author.id}> previously verified but role not found or member not present. Failed detection: win% ${parsed.win_pct ?? 'N/A'}.`);
        }
      } catch (remErr) {
        if (OCR_DEBUG) console.warn('Error removing role on re-scan:', remErr?.message || remErr);
        try { await message.author.send('Your new screenshot does not meet verification requirements. The bot attempted to remove the Comp role but encountered an error. Please contact a server admin.'); } catch (_) {}
        await logToGuild(guild, 'Comp role removal error', `Error removing role for <@${message.author.id}>: ${remErr?.message || remErr}`);
      }

      // mark latest row as not verified
      try {
        await updateLatestRecord(message.author.id, targetGuildId, { verified: false, verified_at: null, flagged: false, flag_reason: null, image_url: url });
      } catch (uErr) { if (OCR_DEBUG) console.warn('Failed to update DB when removing verification:', uErr?.message || uErr); }

      // send updated failure message (single friendly message)
      await logToGuild(guild, 'Verification failed - re-check failed', `User <@${message.author.id}> failed re-check. Detected Win%: ${parsed.win_pct ?? 'N/A'}, Games: ${parsed.games_played ?? 'N/A'}.`);
      return;
    }

    // Not previously verified users who fail checks: new friendly wording (unchanged from your earlier request)
    if (!evalRes.meetsGames) {
      await message.author.send(`Thanks for linking your account — your profile has been saved. You didn't meet the Comp requirements (Win%: ${parsed.win_pct ?? 'N/A'}, Games: ${parsed.games_played ?? 'N/A'}). You can still view other players' stats on the server. Thank you for linking your account.`);
      await logToGuild(guild, 'Verification failed - games', `User <@${message.author.id}> failed games check. Detected: ${parsed.games_played ?? 'N/A'}.`);
      try { await updateLatestRecord(message.author.id, targetGuildId, { image_url: url }); } catch (_) {}
      return;
    }
    if (!evalRes.meetsWin) {
      await message.author.send(`Thanks for linking your account — your profile has been saved. You didn't meet the Comp requirements (Win%: ${parsed.win_pct ?? 'N/A'}, Games: ${parsed.games_played ?? 'N/A'}). You can still view other players' stats on the server. Thank you for linking your account.`);
      await logToGuild(guild, 'Verification failed - win%', `User <@${message.author.id}> failed win% check. Detected: ${parsed.win_pct ?? 'N/A'}.`);
      try { await updateLatestRecord(message.author.id, targetGuildId, { image_url: url }); } catch (_) {}
      return;
    }

    // Passed and no mismatch -> proceed to give role and set verified
    try {
      let roleObj = null;
      const persistedRoleId = await getRoleIdForGuild(guild.id);
      if (persistedRoleId) {
        roleObj = await guild.roles.fetch(persistedRoleId).catch(() => null);
        if (!roleObj) roleObj = await ensureRoleForGuild(guild);
      } else {
        roleObj = await ensureRoleForGuild(guild);
      }

      if (!roleObj) {
        await message.author.send('Congratulations — you passed verification! However, I could not create or find the verification role in the server. Please contact a server admin.');
        await logToGuild(guild, 'Verification passed - role missing', `User <@${message.author.id}> passed but role missing/creation failed.`);
        return;
      }

      const botMember = await guild.members.fetch(client.user.id).catch(() => null);
      const botCanManageRoles = botMember ? botMember.permissions.has(PermissionsBitField.Flags.ManageRoles) : false;
      const botHighestPos = botMember ? botMember.roles.highest.position : -1;

      if (!botCanManageRoles) {
        await message.author.send('Congratulations — you passed verification! I could not add the Comp role automatically because the bot lacks Manage Roles permission in that server. Please contact a server admin to add the Comp Verified role. Detected stats — Win percentage: ' + parsed.win_pct + ', Games: ' + parsed.games_played + '.');
        await logToGuild(guild, 'Verification passed - missing ManageRoles', `User <@${message.author.id}> passed but bot lacks ManageRoles.`);
        // We still keep profile saved but verified remains false for manual assignment
        await updateLatestRecord(message.author.id, targetGuildId, { image_url: url });
        return;
      }
      if (roleObj.position >= botHighestPos) {
        await message.author.send('Congratulations — you passed verification! I could not add the role automatically because the bot role is not higher than the verification role in server role order. Please ask an admin to move the bot role above the verification role. Detected stats — Win percentage: ' + parsed.win_pct + ', Games: ' + parsed.games_played + '.');
        await logToGuild(guild, 'Verification passed - hierarchy issue', `User <@${message.author.id}> passed but bot role lower than verification role.`);
        await updateLatestRecord(message.author.id, targetGuildId, { image_url: url });
        return;
      }

      // add role
      await member.roles.add(roleObj.id, 'Comp Verification passed');

      // update latest record to verified and attach player_tag/platform if present
      const updateObj = {
        verified: true,
        verified_at: new Date().toISOString(),
        expires_at: new Date(Date.now() + REVERIFY_DAYS * 24 * 60 * 60 * 1000).toISOString(),
        player_tag: parsed.player_tag || null,
        platform: parsed.platform || null,
        image_url: url,
        image_hash: parsed.image_hash || image_hash,
        flagged: false,
        flag_reason: null
      };
      await updateLatestRecord(message.author.id, targetGuildId, updateObj);

      // notify user
      await message.author.send(
        `Congratulations — you are verified as a Comp player!\n\nYou have been granted the Comp Verified role which provides access to Comp channels on the server.\n\nDetected stats — Win percentage: ${parsed.win_pct}, Games played: ${parsed.games_played}, Points: ${parsed.points ?? 'N/A'}.\n\nDetected player tag: ${parsed.player_tag ?? 'N/A'} on platform: ${parsed.platform ?? 'N/A'}.\n\nIf you do not see Comp channels, try reloading Discord or contact a server admin. Your verification will remain valid for ${REVERIFY_DAYS} days. Good luck in Comp!`
      );

      await logToGuild(guild, 'Verification success', `User <@${message.author.id}> verified. Win%: ${parsed.win_pct}, Games: ${parsed.games_played}. Tag: ${parsed.player_tag ?? 'N/A'}.`);

      // Post player card to configured channel so others can view
      try {
        const latestAfter = await getLatestRecord(message.author.id, targetGuildId);
        await postPlayerCardToChannel(guild, latestAfter);
      } catch (e) {
        if (OCR_DEBUG) console.warn('Failed to post player card after verification:', e?.message || e);
      }
    } catch (err) {
      console.warn('Role assignment or DB update error:', err?.message || err);
      await message.author.send('You passed but I could not add the role automatically. Contact a server admin.');
      await logToGuild(guild, 'Verification error', `User <@${message.author.id}> passed but error: ${err?.message || err}`);
    }

  } catch (err) {
    console.error('Error processing DM verification:', err?.message || err);
    if (OCR_DEBUG) {
      await message.author.send(`Error while parsing your screenshot: ${err?.message || err}. Check bot logs for details.`);
    } else {
      await message.author.send('Unexpected error while processing your image. Try again later or contact an admin.');
    }
  }
});

client.login(TOKEN).catch(err => {
  console.error('Failed to login:', err?.message || err);
});

// end of file
