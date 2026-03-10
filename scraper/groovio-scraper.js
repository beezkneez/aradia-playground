/**
 * Groovio Lead Scraper
 * Logs in via Puppeteer to get auth cookies, then hits the API directly
 * to pull all leads and sync them to aradia-playground's database.
 */
require("dotenv").config({ path: require("path").join(__dirname, "..", ".env") });
const puppeteer = require("puppeteer");

const GROOVIO_EMAIL = process.env.GROOVIO_EMAIL || "jud.beasley@gmail.com";
const GROOVIO_PASSWORD = process.env.GROOVIO_PASSWORD || "";
const API_BASE = "https://groovio-cms.groovio.com.au/api/v1";
const PLAYGROUND_API = process.env.PLAYGROUND_URL || "http://localhost:3002";

// Status mapping from Groovio internal names to our pipeline
const STATUS_MAP = {
  "new-lead": "new",
  "next-intake": "contacted",
  "in-progress": "interested",
  "trial-started": "trial",
  "trial-completed": "trial",
  "won": "converted",
  "lost": "lost",
};

async function getAuthCookies() {
  console.log("Launching browser for authentication...");
  const browser = await puppeteer.launch({
    headless: "new",
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });

  const page = await browser.newPage();

  // Capture the auth token from API responses
  let authToken = null;
  page.on('request', (request) => {
    const headers = request.headers();
    if (headers.authorization && request.url().includes('groovio-cms')) {
      authToken = headers.authorization;
    }
  });

  await page.goto("https://groovio.com.au/login", { waitUntil: "networkidle2", timeout: 30000 });
  await new Promise(r => setTimeout(r, 2000));

  // Fill login form
  await page.click('#email');
  await page.keyboard.type(GROOVIO_EMAIL, { delay: 20 });
  await page.click('#password');
  await page.keyboard.type(GROOVIO_PASSWORD, { delay: 20 });
  await page.keyboard.press('Enter');

  // Wait for login to complete
  await new Promise(r => setTimeout(r, 8000));

  const currentUrl = page.url();
  if (currentUrl.includes('login')) {
    // Check for 2FA
    const pageText = await page.evaluate(() => document.body.innerText);
    if (pageText.includes('OTP') || pageText.includes('verification') || pageText.includes('code')) {
      console.log("2FA detected! Please enter the code manually...");
      // Wait longer for manual 2FA entry
      await new Promise(r => setTimeout(r, 60000));
    } else {
      throw new Error("Login failed - still on login page");
    }
  }

  // Navigate to leads to capture auth token
  await page.goto("https://groovio.com.au/leads", { waitUntil: "networkidle2", timeout: 30000 });
  await new Promise(r => setTimeout(r, 3000));

  // Get cookies
  const cookies = await page.cookies();

  // Also get localStorage tokens
  const storage = await page.evaluate(() => {
    const items = {};
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      items[key] = localStorage.getItem(key);
    }
    return items;
  });

  await browser.close();

  return { cookies, authToken, storage };
}

async function fetchLeads(cookies, authToken) {
  console.log("Fetching leads from Groovio API...");
  const allLeads = [];
  let page = 1;
  let hasNext = true;

  // Build cookie string
  const cookieStr = cookies.map(c => `${c.name}=${c.value}`).join('; ');

  while (hasNext) {
    const url = `${API_BASE}/studio_leads/${page > 1 ? '?page=' + page : ''}`;
    console.log(`  Fetching page ${page}...`);

    const headers = {
      'Cookie': cookieStr,
      'Accept': 'application/json',
      'Referer': 'https://groovio.com.au/leads',
      'Origin': 'https://groovio.com.au',
    };
    if (authToken) headers['Authorization'] = authToken;

    const res = await fetch(url, { headers });
    const data = await res.json();

    if (data.results) {
      allLeads.push(...data.results);
      hasNext = !!data.next;
      page++;
    } else {
      hasNext = false;
    }
  }

  console.log(`  Total leads fetched: ${allLeads.length}`);
  return allLeads;
}

async function syncToPlayground(leads) {
  console.log("Syncing leads to Aradia Marketing Hub...");

  const mapped = leads.map(lead => ({
    name: lead.name || '',
    email: lead.email || '',
    phone: lead.phone || '',
    source: lead.source === 'student' ? 'groovio' : (lead.source || 'groovio'),
    location: '',  // Groovio doesn't seem to include location per lead
    notes: lead.notes || '',
    status: STATUS_MAP[lead.status] || 'new',
  }));

  // Send in batches of 100
  const batchSize = 100;
  let totalImported = 0;
  for (let i = 0; i < mapped.length; i += batchSize) {
    const batch = mapped.slice(i, i + batchSize);
    try {
      const res = await fetch(`${PLAYGROUND_API}/api/leads/import`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ leads: batch }),
      });
      const result = await res.json();
      totalImported += result.imported || 0;
      console.log(`  Batch ${Math.floor(i/batchSize) + 1}: imported ${result.imported} leads`);
    } catch (err) {
      console.error(`  Batch error:`, err.message);
    }
  }

  console.log(`\nSync complete! Total imported: ${totalImported} / ${leads.length}`);
  return totalImported;
}

async function run() {
  try {
    const { cookies, authToken } = await getAuthCookies();
    const leads = await fetchLeads(cookies, authToken);
    const imported = await syncToPlayground(leads);
    console.log(`\nDone! ${imported} leads synced to Marketing Hub.`);
  } catch (err) {
    console.error("Scraper error:", err.message);
    process.exit(1);
  }
}

// Export for use as module or run directly
if (require.main === module) {
  run();
} else {
  module.exports = { getAuthCookies, fetchLeads, syncToPlayground, run };
}
