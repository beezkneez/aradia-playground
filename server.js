require("dotenv").config();
const express = require("express");
const { Pool } = require("pg");
const { Resend } = require("resend");
const { v4: uuidv4 } = require("uuid");
const path = require("path");
const cron = require("node-cron");

const app = express();
app.use(express.json({ limit: "10mb" }));
app.use(express.static(path.join(__dirname, "public")));

// ─────────────────────────────────────────
//  CONFIG
// ─────────────────────────────────────────
const CONFIG = {
  BRAND_NAME: "Aradia Marketing Hub",
  LOCATIONS: ["South Edmonton", "Kingsway Edmonton", "St. Albert", "Spruce Grove"],
  FROM_EMAIL: process.env.RESEND_FROM || "Aradia Marketing <marketing@aradiafitness.app>",
};

// ─────────────────────────────────────────
//  POSTGRES
// ─────────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL && process.env.DATABASE_URL.includes("railway")
    ? { rejectUnauthorized: false }
    : false,
});

async function query(sql, params = [], retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    let client;
    try {
      client = await pool.connect();
      const res = await client.query(sql, params);
      return res;
    } catch (err) {
      const isTransient = err.code === 'EAI_AGAIN' || err.code === 'ENOTFOUND' ||
                          err.code === 'ECONNREFUSED' || err.code === 'ECONNRESET';
      if (isTransient && attempt < retries) {
        await new Promise(r => setTimeout(r, attempt * 2000));
      } else {
        throw err;
      }
    } finally {
      if (client) client.release();
    }
  }
}

// ─────────────────────────────────────────
//  RESEND EMAIL
// ─────────────────────────────────────────
let resend = null;
if (process.env.RESEND_API_KEY) {
  resend = new Resend(process.env.RESEND_API_KEY);
  console.log("Resend configured ✅");
}

// ─────────────────────────────────────────
//  DATABASE SETUP
// ─────────────────────────────────────────
async function setupDatabase() {
  // ── Leads ──
  await query(`
    CREATE TABLE IF NOT EXISTS leads (
      id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      name        TEXT,
      email       TEXT,
      phone       TEXT,
      source      TEXT DEFAULT 'manual',
      status      TEXT DEFAULT 'new',
      location    TEXT,
      notes       TEXT DEFAULT '',
      tags        TEXT DEFAULT '',
      created_at  TIMESTAMPTZ DEFAULT NOW(),
      updated_at  TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // ── Lead Activity Log ──
  await query(`
    CREATE TABLE IF NOT EXISTS lead_activity (
      id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      lead_id     TEXT REFERENCES leads(id) ON DELETE CASCADE,
      type        TEXT NOT NULL,
      detail      TEXT,
      created_at  TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // ── Email Templates ──
  await query(`
    CREATE TABLE IF NOT EXISTS email_templates (
      id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      name        TEXT NOT NULL,
      subject     TEXT NOT NULL,
      body        TEXT NOT NULL,
      category    TEXT DEFAULT 'general',
      created_at  TIMESTAMPTZ DEFAULT NOW(),
      updated_at  TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // ── Email Campaigns ──
  await query(`
    CREATE TABLE IF NOT EXISTS campaigns (
      id              TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      name            TEXT NOT NULL,
      template_id     TEXT REFERENCES email_templates(id),
      status          TEXT DEFAULT 'draft',
      scheduled_at    TIMESTAMPTZ,
      sent_at         TIMESTAMPTZ,
      recipient_filter TEXT DEFAULT '{}',
      total_sent      INTEGER DEFAULT 0,
      total_opened    INTEGER DEFAULT 0,
      total_clicked   INTEGER DEFAULT 0,
      created_at      TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // ── Campaign Recipients ──
  await query(`
    CREATE TABLE IF NOT EXISTS campaign_recipients (
      id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      campaign_id TEXT REFERENCES campaigns(id) ON DELETE CASCADE,
      lead_id     TEXT REFERENCES leads(id) ON DELETE CASCADE,
      email       TEXT NOT NULL,
      status      TEXT DEFAULT 'pending',
      sent_at     TIMESTAMPTZ,
      opened_at   TIMESTAMPTZ,
      clicked_at  TIMESTAMPTZ
    )
  `);

  // ── Content Calendar ──
  await query(`
    CREATE TABLE IF NOT EXISTS calendar_events (
      id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      title       TEXT NOT NULL,
      type        TEXT DEFAULT 'post',
      platform    TEXT DEFAULT 'instagram',
      location    TEXT,
      scheduled_date DATE NOT NULL,
      scheduled_time TIME,
      status      TEXT DEFAULT 'planned',
      content     TEXT DEFAULT '',
      image_url   TEXT,
      notes       TEXT DEFAULT '',
      created_at  TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // ── Members (synced from main app or manual) ──
  await query(`
    CREATE TABLE IF NOT EXISTS members (
      id              TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      name            TEXT,
      email           TEXT UNIQUE,
      phone           TEXT,
      location        TEXT,
      join_date       DATE,
      birthday        DATE,
      status          TEXT DEFAULT 'active',
      last_attendance DATE,
      attendance_count INTEGER DEFAULT 0,
      referral_source TEXT,
      referred_by     TEXT,
      notes           TEXT DEFAULT '',
      created_at      TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // ── Referrals ──
  await query(`
    CREATE TABLE IF NOT EXISTS referrals (
      id              TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      referrer_id     TEXT REFERENCES members(id) ON DELETE SET NULL,
      referred_name   TEXT,
      referred_email  TEXT,
      status          TEXT DEFAULT 'pending',
      reward_given    BOOLEAN DEFAULT FALSE,
      created_at      TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // ── Reviews ──
  await query(`
    CREATE TABLE IF NOT EXISTS reviews (
      id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      platform    TEXT DEFAULT 'google',
      author      TEXT,
      rating      INTEGER,
      content     TEXT,
      location    TEXT,
      response    TEXT,
      responded   BOOLEAN DEFAULT FALSE,
      review_date TIMESTAMPTZ,
      created_at  TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // ── Promos ──
  await query(`
    CREATE TABLE IF NOT EXISTS promos (
      id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      name        TEXT NOT NULL,
      code        TEXT UNIQUE,
      type        TEXT DEFAULT 'discount',
      value       TEXT,
      description TEXT DEFAULT '',
      start_date  DATE,
      end_date    DATE,
      location    TEXT,
      max_uses    INTEGER,
      current_uses INTEGER DEFAULT 0,
      active      BOOLEAN DEFAULT TRUE,
      trackable_url TEXT,
      created_at  TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // ── Milestones ──
  await query(`
    CREATE TABLE IF NOT EXISTS milestones (
      id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      member_id   TEXT REFERENCES members(id) ON DELETE CASCADE,
      type        TEXT NOT NULL,
      title       TEXT,
      milestone_date DATE,
      notified    BOOLEAN DEFAULT FALSE,
      created_at  TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // Add unique index on email for dedup (ignore nulls/empty)
  await query(`CREATE UNIQUE INDEX IF NOT EXISTS leads_email_unique ON leads (email) WHERE email IS NOT NULL AND email != ''`).catch(()=>{});
  // Add groovio_id column for tracking synced leads
  await query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS groovio_id INTEGER`).catch(()=>{});
  await query(`CREATE UNIQUE INDEX IF NOT EXISTS leads_groovio_id_unique ON leads (groovio_id) WHERE groovio_id IS NOT NULL`).catch(()=>{});

  console.log("Database tables ready ✅");
}

// ═══════════════════════════════════════════
//  API ROUTES
// ═══════════════════════════════════════════

// ── LEADS ──────────────────────────────────
app.get("/api/leads", async (req, res) => {
  try {
    const { status, source, search } = req.query;
    let sql = "SELECT * FROM leads WHERE 1=1";
    const params = [];
    if (status) { params.push(status); sql += ` AND status = $${params.length}`; }
    if (source) { params.push(source); sql += ` AND source = $${params.length}`; }
    if (search) { params.push(`%${search}%`); sql += ` AND (name ILIKE $${params.length} OR email ILIKE $${params.length})`; }
    sql += " ORDER BY created_at DESC";
    const result = await query(sql, params);
    res.json(result.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/leads", async (req, res) => {
  try {
    const { name, email, phone, source, status, location, notes, tags } = req.body;
    const result = await query(
      `INSERT INTO leads (name, email, phone, source, status, location, notes, tags)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
      [name, email, phone, source || 'manual', status || 'new', location, notes, tags]
    );
    await query(`INSERT INTO lead_activity (lead_id, type, detail) VALUES ($1, 'created', 'Lead added')`,
      [result.rows[0].id]);
    res.json(result.rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put("/api/leads/:id", async (req, res) => {
  try {
    const { name, email, phone, source, status, location, notes, tags } = req.body;
    const old = await query("SELECT status FROM leads WHERE id = $1", [req.params.id]);
    const result = await query(
      `UPDATE leads SET name=$1, email=$2, phone=$3, source=$4, status=$5, location=$6,
       notes=$7, tags=$8, updated_at=NOW() WHERE id=$9 RETURNING *`,
      [name, email, phone, source, status, location, notes, tags, req.params.id]
    );
    if (old.rows[0] && old.rows[0].status !== status) {
      await query(`INSERT INTO lead_activity (lead_id, type, detail) VALUES ($1, 'status_change', $2)`,
        [req.params.id, `Status changed from ${old.rows[0].status} to ${status}`]);
    }
    res.json(result.rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete("/api/leads/:id", async (req, res) => {
  try {
    await query("DELETE FROM leads WHERE id = $1", [req.params.id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/leads/import", async (req, res) => {
  try {
    const { leads } = req.body;
    let imported = 0, updated = 0, skipped = 0;
    for (const lead of leads) {
      if (!lead.email && !lead.name) { skipped++; continue; }
      if (lead.groovio_id) {
        // Upsert by groovio_id
        const result = await query(
          `INSERT INTO leads (name, email, phone, source, status, location, notes, groovio_id)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
           ON CONFLICT (groovio_id) DO UPDATE SET
             name=EXCLUDED.name, phone=EXCLUDED.phone, notes=EXCLUDED.notes,
             status=EXCLUDED.status, updated_at=NOW()
           RETURNING (xmax = 0) AS inserted`,
          [lead.name, lead.email, lead.phone, lead.source || 'groovio', lead.status || 'new', lead.location, lead.notes, lead.groovio_id]
        );
        if (result.rows[0]?.inserted) imported++; else updated++;
      } else if (lead.email) {
        // Upsert by email
        const result = await query(
          `INSERT INTO leads (name, email, phone, source, location, notes)
           VALUES ($1,$2,$3,$4,$5,$6)
           ON CONFLICT ((email)) WHERE email IS NOT NULL AND email != '' DO NOTHING`,
          [lead.name, lead.email, lead.phone, lead.source || 'import', lead.location, lead.notes]
        );
        imported++;
      } else {
        await query(
          `INSERT INTO leads (name, phone, source, location, notes)
           VALUES ($1,$2,$3,$4,$5)`,
          [lead.name, lead.phone, lead.source || 'import', lead.location, lead.notes]
        );
        imported++;
      }
    }
    res.json({ imported, updated, skipped });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get("/api/leads/:id/activity", async (req, res) => {
  try {
    const result = await query(
      "SELECT * FROM lead_activity WHERE lead_id = $1 ORDER BY created_at DESC", [req.params.id]);
    res.json(result.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── LEAD STATS ──
app.get("/api/leads/stats/funnel", async (req, res) => {
  try {
    const result = await query(
      `SELECT status, COUNT(*) as count FROM leads GROUP BY status ORDER BY
       CASE status WHEN 'new' THEN 1 WHEN 'contacted' THEN 2 WHEN 'interested' THEN 3
       WHEN 'trial' THEN 4 WHEN 'converted' THEN 5 WHEN 'lost' THEN 6 END`
    );
    res.json(result.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── EMAIL LEADS ──
app.post("/api/leads/email", async (req, res) => {
  try {
    if (!resend) return res.status(400).json({ error: "Email not configured" });
    const { leadIds, subject, body } = req.body;
    const leadsResult = await query("SELECT * FROM leads WHERE id = ANY($1)", [leadIds]);
    let sent = 0;
    for (const lead of leadsResult.rows) {
      if (!lead.email) continue;
      await resend.emails.send({
        from: CONFIG.FROM_EMAIL,
        to: lead.email,
        subject,
        html: body.replace(/\{name\}/g, lead.name || 'there'),
      });
      await query(`INSERT INTO lead_activity (lead_id, type, detail) VALUES ($1, 'email_sent', $2)`,
        [lead.id, `Email: ${subject}`]);
      sent++;
    }
    res.json({ sent });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── EMAIL TEMPLATES ────────────────────────
app.get("/api/templates", async (req, res) => {
  try {
    const result = await query("SELECT * FROM email_templates ORDER BY created_at DESC");
    res.json(result.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/templates", async (req, res) => {
  try {
    const { name, subject, body, category } = req.body;
    const result = await query(
      `INSERT INTO email_templates (name, subject, body, category) VALUES ($1,$2,$3,$4) RETURNING *`,
      [name, subject, body, category]
    );
    res.json(result.rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put("/api/templates/:id", async (req, res) => {
  try {
    const { name, subject, body, category } = req.body;
    const result = await query(
      `UPDATE email_templates SET name=$1, subject=$2, body=$3, category=$4, updated_at=NOW()
       WHERE id=$5 RETURNING *`,
      [name, subject, body, category, req.params.id]
    );
    res.json(result.rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete("/api/templates/:id", async (req, res) => {
  try {
    await query("DELETE FROM email_templates WHERE id = $1", [req.params.id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── CAMPAIGNS ──────────────────────────────
app.get("/api/campaigns", async (req, res) => {
  try {
    const result = await query(
      `SELECT c.*, t.name as template_name, t.subject as template_subject
       FROM campaigns c LEFT JOIN email_templates t ON c.template_id = t.id
       ORDER BY c.created_at DESC`
    );
    res.json(result.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/campaigns", async (req, res) => {
  try {
    const { name, template_id, recipient_filter, scheduled_at } = req.body;
    const result = await query(
      `INSERT INTO campaigns (name, template_id, recipient_filter, scheduled_at)
       VALUES ($1,$2,$3,$4) RETURNING *`,
      [name, template_id, JSON.stringify(recipient_filter || {}), scheduled_at]
    );
    res.json(result.rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/campaigns/:id/send", async (req, res) => {
  try {
    if (!resend) return res.status(400).json({ error: "Email not configured" });
    const campaign = (await query("SELECT * FROM campaigns WHERE id = $1", [req.params.id])).rows[0];
    if (!campaign) return res.status(404).json({ error: "Campaign not found" });
    const template = (await query("SELECT * FROM email_templates WHERE id = $1", [campaign.template_id])).rows[0];
    if (!template) return res.status(404).json({ error: "Template not found" });

    const filter = JSON.parse(campaign.recipient_filter || '{}');
    let sql = "SELECT * FROM leads WHERE email IS NOT NULL AND email != ''";
    const params = [];
    if (filter.status) { params.push(filter.status); sql += ` AND status = $${params.length}`; }
    if (filter.location) { params.push(filter.location); sql += ` AND location = $${params.length}`; }
    const leads = (await query(sql, params)).rows;

    let sent = 0;
    for (const lead of leads) {
      try {
        await resend.emails.send({
          from: CONFIG.FROM_EMAIL,
          to: lead.email,
          subject: template.subject.replace(/\{name\}/g, lead.name || 'there'),
          html: template.body.replace(/\{name\}/g, lead.name || 'there'),
        });
        await query(
          `INSERT INTO campaign_recipients (campaign_id, lead_id, email, status, sent_at)
           VALUES ($1,$2,$3,'sent',NOW())`,
          [req.params.id, lead.id, lead.email]
        );
        sent++;
      } catch (e) {
        await query(
          `INSERT INTO campaign_recipients (campaign_id, lead_id, email, status)
           VALUES ($1,$2,$3,'failed')`,
          [req.params.id, lead.id, lead.email]
        );
      }
    }
    await query("UPDATE campaigns SET status='sent', sent_at=NOW(), total_sent=$1 WHERE id=$2",
      [sent, req.params.id]);
    res.json({ sent });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete("/api/campaigns/:id", async (req, res) => {
  try {
    await query("DELETE FROM campaigns WHERE id = $1", [req.params.id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── CONTENT CALENDAR ───────────────────────
app.get("/api/calendar", async (req, res) => {
  try {
    const { month, year } = req.query;
    let sql = "SELECT * FROM calendar_events";
    const params = [];
    if (month && year) {
      params.push(parseInt(year), parseInt(month));
      sql += ` WHERE EXTRACT(YEAR FROM scheduled_date) = $1 AND EXTRACT(MONTH FROM scheduled_date) = $2`;
    }
    sql += " ORDER BY scheduled_date, scheduled_time";
    const result = await query(sql, params);
    res.json(result.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/calendar", async (req, res) => {
  try {
    const { title, type, platform, location, scheduled_date, scheduled_time, content, notes } = req.body;
    const result = await query(
      `INSERT INTO calendar_events (title, type, platform, location, scheduled_date, scheduled_time, content, notes)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
      [title, type, platform, location, scheduled_date, scheduled_time, content, notes]
    );
    res.json(result.rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put("/api/calendar/:id", async (req, res) => {
  try {
    const { title, type, platform, location, scheduled_date, scheduled_time, status, content, notes } = req.body;
    const result = await query(
      `UPDATE calendar_events SET title=$1, type=$2, platform=$3, location=$4,
       scheduled_date=$5, scheduled_time=$6, status=$7, content=$8, notes=$9
       WHERE id=$10 RETURNING *`,
      [title, type, platform, location, scheduled_date, scheduled_time, status, content, notes, req.params.id]
    );
    res.json(result.rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete("/api/calendar/:id", async (req, res) => {
  try {
    await query("DELETE FROM calendar_events WHERE id = $1", [req.params.id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── MEMBERS ────────────────────────────────
app.get("/api/members", async (req, res) => {
  try {
    const { status, search, location } = req.query;
    let sql = "SELECT * FROM members WHERE 1=1";
    const params = [];
    if (status) { params.push(status); sql += ` AND status = $${params.length}`; }
    if (location) { params.push(location); sql += ` AND location = $${params.length}`; }
    if (search) { params.push(`%${search}%`); sql += ` AND (name ILIKE $${params.length} OR email ILIKE $${params.length})`; }
    sql += " ORDER BY name";
    const result = await query(sql, params);
    res.json(result.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/members", async (req, res) => {
  try {
    const { name, email, phone, location, join_date, birthday, status, referral_source, referred_by, notes } = req.body;
    const result = await query(
      `INSERT INTO members (name, email, phone, location, join_date, birthday, status, referral_source, referred_by, notes)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *`,
      [name, email, phone, location, join_date, birthday, status || 'active', referral_source, referred_by, notes]
    );
    res.json(result.rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put("/api/members/:id", async (req, res) => {
  try {
    const { name, email, phone, location, join_date, birthday, status, last_attendance, attendance_count, referral_source, referred_by, notes } = req.body;
    const result = await query(
      `UPDATE members SET name=$1, email=$2, phone=$3, location=$4, join_date=$5, birthday=$6,
       status=$7, last_attendance=$8, attendance_count=$9, referral_source=$10, referred_by=$11, notes=$12
       WHERE id=$13 RETURNING *`,
      [name, email, phone, location, join_date, birthday, status, last_attendance, attendance_count, referral_source, referred_by, notes, req.params.id]
    );
    res.json(result.rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── RETENTION ──
app.get("/api/members/retention/at-risk", async (req, res) => {
  try {
    const result = await query(
      `SELECT * FROM members
       WHERE status = 'active'
       AND (last_attendance < NOW() - INTERVAL '14 days' OR last_attendance IS NULL)
       ORDER BY last_attendance ASC NULLS FIRST`
    );
    res.json(result.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── REFERRALS ──────────────────────────────
app.get("/api/referrals", async (req, res) => {
  try {
    const result = await query(
      `SELECT r.*, m.name as referrer_name, m.email as referrer_email
       FROM referrals r LEFT JOIN members m ON r.referrer_id = m.id
       ORDER BY r.created_at DESC`
    );
    res.json(result.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/referrals", async (req, res) => {
  try {
    const { referrer_id, referred_name, referred_email } = req.body;
    const result = await query(
      `INSERT INTO referrals (referrer_id, referred_name, referred_email) VALUES ($1,$2,$3) RETURNING *`,
      [referrer_id, referred_name, referred_email]
    );
    res.json(result.rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put("/api/referrals/:id", async (req, res) => {
  try {
    const { status, reward_given } = req.body;
    const result = await query(
      `UPDATE referrals SET status=$1, reward_given=$2 WHERE id=$3 RETURNING *`,
      [status, reward_given, req.params.id]
    );
    res.json(result.rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Top Referrers ──
app.get("/api/referrals/top", async (req, res) => {
  try {
    const result = await query(
      `SELECT m.id, m.name, m.email, COUNT(r.id) as referral_count,
       SUM(CASE WHEN r.status = 'converted' THEN 1 ELSE 0 END) as converted_count
       FROM members m JOIN referrals r ON m.id = r.referrer_id
       GROUP BY m.id, m.name, m.email ORDER BY referral_count DESC LIMIT 20`
    );
    res.json(result.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── REVIEWS ────────────────────────────────
app.get("/api/reviews", async (req, res) => {
  try {
    const { platform, responded } = req.query;
    let sql = "SELECT * FROM reviews WHERE 1=1";
    const params = [];
    if (platform) { params.push(platform); sql += ` AND platform = $${params.length}`; }
    if (responded !== undefined) { params.push(responded === 'true'); sql += ` AND responded = $${params.length}`; }
    sql += " ORDER BY review_date DESC NULLS LAST, created_at DESC";
    const result = await query(sql, params);
    res.json(result.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/reviews", async (req, res) => {
  try {
    const { platform, author, rating, content, location, review_date } = req.body;
    const result = await query(
      `INSERT INTO reviews (platform, author, rating, content, location, review_date)
       VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
      [platform, author, rating, content, location, review_date]
    );
    res.json(result.rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put("/api/reviews/:id/respond", async (req, res) => {
  try {
    const { response } = req.body;
    const result = await query(
      `UPDATE reviews SET response=$1, responded=TRUE WHERE id=$2 RETURNING *`,
      [response, req.params.id]
    );
    res.json(result.rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Review Stats ──
app.get("/api/reviews/stats", async (req, res) => {
  try {
    const result = await query(
      `SELECT platform, location,
       COUNT(*) as total, ROUND(AVG(rating),1) as avg_rating,
       SUM(CASE WHEN responded THEN 1 ELSE 0 END) as responded_count
       FROM reviews GROUP BY platform, location ORDER BY platform, location`
    );
    res.json(result.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── PROMOS ─────────────────────────────────
app.get("/api/promos", async (req, res) => {
  try {
    const result = await query("SELECT * FROM promos ORDER BY created_at DESC");
    res.json(result.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/promos", async (req, res) => {
  try {
    const { name, code, type, value, description, start_date, end_date, location, max_uses } = req.body;
    const result = await query(
      `INSERT INTO promos (name, code, type, value, description, start_date, end_date, location, max_uses)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *`,
      [name, code, type, value, description, start_date, end_date, location, max_uses]
    );
    res.json(result.rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put("/api/promos/:id", async (req, res) => {
  try {
    const { name, code, type, value, description, start_date, end_date, location, max_uses, active } = req.body;
    const result = await query(
      `UPDATE promos SET name=$1, code=$2, type=$3, value=$4, description=$5,
       start_date=$6, end_date=$7, location=$8, max_uses=$9, active=$10
       WHERE id=$11 RETURNING *`,
      [name, code, type, value, description, start_date, end_date, location, max_uses, active, req.params.id]
    );
    res.json(result.rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete("/api/promos/:id", async (req, res) => {
  try {
    await query("DELETE FROM promos WHERE id = $1", [req.params.id]);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── MILESTONES ─────────────────────────────
app.get("/api/milestones", async (req, res) => {
  try {
    const result = await query(
      `SELECT ms.*, m.name as member_name, m.email as member_email
       FROM milestones ms JOIN members m ON ms.member_id = m.id
       ORDER BY ms.milestone_date ASC`
    );
    res.json(result.rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get("/api/milestones/upcoming", async (req, res) => {
  try {
    // Birthdays in next 30 days
    const birthdays = await query(
      `SELECT id, name, email, birthday, location FROM members
       WHERE birthday IS NOT NULL AND status = 'active'
       AND (
         (EXTRACT(MONTH FROM birthday) = EXTRACT(MONTH FROM CURRENT_DATE)
          AND EXTRACT(DAY FROM birthday) >= EXTRACT(DAY FROM CURRENT_DATE))
         OR
         (EXTRACT(MONTH FROM birthday) = EXTRACT(MONTH FROM CURRENT_DATE + INTERVAL '30 days')
          AND EXTRACT(DAY FROM birthday) <= EXTRACT(DAY FROM CURRENT_DATE + INTERVAL '30 days'))
       )
       ORDER BY EXTRACT(MONTH FROM birthday), EXTRACT(DAY FROM birthday)`
    );
    // Anniversaries in next 30 days
    const anniversaries = await query(
      `SELECT id, name, email, join_date, location,
       EXTRACT(YEAR FROM AGE(CURRENT_DATE, join_date)) as years
       FROM members
       WHERE join_date IS NOT NULL AND status = 'active'
       AND (
         (EXTRACT(MONTH FROM join_date) = EXTRACT(MONTH FROM CURRENT_DATE)
          AND EXTRACT(DAY FROM join_date) >= EXTRACT(DAY FROM CURRENT_DATE))
         OR
         (EXTRACT(MONTH FROM join_date) = EXTRACT(MONTH FROM CURRENT_DATE + INTERVAL '30 days')
          AND EXTRACT(DAY FROM join_date) <= EXTRACT(DAY FROM CURRENT_DATE + INTERVAL '30 days'))
       )
       ORDER BY EXTRACT(MONTH FROM join_date), EXTRACT(DAY FROM join_date)`
    );
    res.json({ birthdays: birthdays.rows, anniversaries: anniversaries.rows });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── DASHBOARD STATS ────────────────────────
app.get("/api/dashboard", async (req, res) => {
  try {
    const [leads, members, campaigns, reviews, promos, referrals] = await Promise.all([
      query("SELECT status, COUNT(*) as count FROM leads GROUP BY status"),
      query("SELECT status, COUNT(*) as count FROM members GROUP BY status"),
      query("SELECT status, COUNT(*) as count FROM campaigns GROUP BY status"),
      query("SELECT COUNT(*) as total, ROUND(AVG(rating),1) as avg_rating, SUM(CASE WHEN NOT responded THEN 1 ELSE 0 END) as unresponded FROM reviews"),
      query("SELECT COUNT(*) as total, SUM(CASE WHEN active AND (end_date IS NULL OR end_date >= CURRENT_DATE) THEN 1 ELSE 0 END) as active FROM promos"),
      query("SELECT COUNT(*) as total, SUM(CASE WHEN status='converted' THEN 1 ELSE 0 END) as converted FROM referrals"),
    ]);
    res.json({
      leads: leads.rows,
      members: members.rows,
      campaigns: campaigns.rows,
      reviews: reviews.rows[0],
      promos: promos.rows[0],
      referrals: referrals.rows[0],
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── IMAGE PROXY (for social card creator) ──
app.post("/api/generateImage", async (req, res) => {
  try {
    const imageUrl = process.env.IMAGE_GENERATOR_URL;
    if (!imageUrl) return res.status(400).json({ error: "IMAGE_GENERATOR_URL not set" });
    const fetch = (await import('node-fetch')).default;
    const resp = await fetch(`${imageUrl}/generateImage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(req.body),
    });
    const data = await resp.buffer();
    res.set('Content-Type', 'image/png');
    res.send(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── SEND SINGLE EMAIL ──
app.post("/api/email/send", async (req, res) => {
  try {
    if (!resend) return res.status(400).json({ error: "Email not configured" });
    const { to, subject, html } = req.body;
    await resend.emails.send({ from: CONFIG.FROM_EMAIL, to, subject, html });
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── GROOVIO SCRAPER ────────────────────────
let scrapeInProgress = false;

app.post("/api/scrape/groovio", async (req, res) => {
  if (scrapeInProgress) return res.status(409).json({ error: "Scrape already in progress" });
  scrapeInProgress = true;

  // Respond immediately, scrape runs in background
  res.json({ status: "started", message: "Groovio scrape started. Check /api/scrape/groovio/status for progress." });

  try {
    const { getAuthCookies, fetchLeads } = require("./scraper/groovio-scraper");
    const { cookies, authToken } = await getAuthCookies();
    const leads = await fetchLeads(cookies, authToken);

    const STATUS_MAP = {
      "new-lead": "new",
      "next-intake": "contacted",
      "in-progress": "interested",
      "trial-started": "trial",
      "trial-completed": "trial",
      "won": "converted",
      "lost": "lost",
    };

    let imported = 0, updated = 0;
    for (const lead of leads) {
      if (!lead.email && !lead.name) continue;
      try {
        const result = await query(
          `INSERT INTO leads (name, email, phone, source, status, notes, groovio_id)
           VALUES ($1,$2,$3,'groovio',$4,$5,$6)
           ON CONFLICT (groovio_id) DO UPDATE SET
             name=EXCLUDED.name, phone=EXCLUDED.phone, notes=EXCLUDED.notes,
             status=EXCLUDED.status, updated_at=NOW()
           RETURNING (xmax = 0) AS inserted`,
          [lead.name, lead.email, lead.phone, STATUS_MAP[lead.status] || 'new', lead.notes || '', lead.id]
        );
        if (result.rows[0]?.inserted) imported++; else updated++;
      } catch (e) { /* skip dupes */ }
    }

    lastScrapeResult = { success: true, imported, updated, total: leads.length, completedAt: new Date().toISOString() };
    console.log(`Groovio scrape complete: ${imported} new, ${updated} updated, ${leads.length} total`);
  } catch (err) {
    lastScrapeResult = { success: false, error: err.message, completedAt: new Date().toISOString() };
    console.error("Groovio scrape failed:", err.message);
  } finally {
    scrapeInProgress = false;
  }
});

let lastScrapeResult = null;

app.get("/api/scrape/groovio/status", (req, res) => {
  res.json({
    inProgress: scrapeInProgress,
    lastResult: lastScrapeResult,
  });
});

// ── START ──────────────────────────────────
const PORT = process.env.PORT || 3002;
setupDatabase()
  .then(() => {
    app.listen(PORT, () => console.log(`Aradia Marketing Hub running on port ${PORT}`));
  })
  .catch(err => {
    console.error("DB setup failed:", err.message);
    app.listen(PORT, () => console.log(`Server running on port ${PORT} (DB not connected)`));
  });
