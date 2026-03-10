/**
 * Groovio Lead Scraper
 * Uses Puppeteer to login and intercept API responses directly.
 * No need to reverse-engineer auth headers — the React app handles it.
 */
require("dotenv").config({ path: require("path").join(__dirname, "..", ".env") });
const puppeteer = require("puppeteer");

const GROOVIO_EMAIL = process.env.GROOVIO_EMAIL || "jud.beasley@gmail.com";
const GROOVIO_PASSWORD = process.env.GROOVIO_PASSWORD || "";

const STATUS_MAP = {
  "new-lead": "new",
  "next-intake": "contacted",
  "in-progress": "interested",
  "trial-started": "trial",
  "trial-completed": "trial",
  "won": "converted",
  "lost": "lost",
};

async function run() {
  if (!GROOVIO_PASSWORD) throw new Error("GROOVIO_PASSWORD not set in environment");

  console.log("Launching browser...");
  const browser = await puppeteer.launch({
    headless: "new",
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
    ],
  });

  try {
    const page = await browser.newPage();

    // Set a real user agent
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36');

    // Collect lead data from intercepted API responses
    const allLeads = [];
    let leadsPromiseResolve;
    let pagesReceived = 0;
    let totalExpected = 0;

    page.on('response', async (response) => {
      const url = response.url();
      if (url.includes('studio_leads') && !url.includes('new_leads_count') && !url.includes('column_names') && response.request().method() === 'GET') {
        try {
          const data = await response.json();
          if (data.results) {
            allLeads.push(...data.results);
            pagesReceived++;
            totalExpected = data.count || totalExpected;
            console.log(`  Captured page ${pagesReceived}: ${data.results.length} leads (total: ${allLeads.length}/${totalExpected})`);
          }
        } catch (e) { /* ignore non-JSON responses */ }
      }
    });

    // Login
    console.log("Navigating to login...");
    await page.goto("https://groovio.com.au/login", { waitUntil: "networkidle2", timeout: 30000 });
    await new Promise(r => setTimeout(r, 2000));

    console.log("Filling credentials...");
    await page.click('#email');
    await page.keyboard.type(GROOVIO_EMAIL, { delay: 15 });
    await page.click('#password');
    await page.keyboard.type(GROOVIO_PASSWORD, { delay: 15 });
    await page.keyboard.press('Enter');

    // Wait for login redirect
    console.log("Waiting for login...");
    await new Promise(r => setTimeout(r, 8000));

    if (page.url().includes('login')) {
      // Check for error
      const pageText = await page.evaluate(() => document.body.innerText.substring(0, 500));
      throw new Error(`Login failed. Page URL: ${page.url()}. Content: ${pageText.substring(0, 200)}`);
    }

    console.log("Login successful! URL:", page.url());

    // Navigate to leads page — this triggers the API calls
    console.log("Navigating to leads page...");
    await page.goto("https://groovio.com.au/leads", { waitUntil: "networkidle2", timeout: 30000 });
    await new Promise(r => setTimeout(r, 5000));

    // The leads page loads page 1 automatically.
    // If there are more pages, we need to trigger pagination.
    // Check if we got all leads
    if (allLeads.length < totalExpected) {
      console.log(`Got ${allLeads.length}/${totalExpected}. Fetching remaining pages via scrolling/pagination...`);

      // Try to click "next page" or scroll to load more
      // First, let's see if there's a pagination control
      const hasNextPage = await page.evaluate(() => {
        const nextBtn = document.querySelector('.ant-pagination-next:not(.ant-pagination-disabled)');
        return !!nextBtn;
      });

      if (hasNextPage) {
        while (allLeads.length < totalExpected) {
          const before = allLeads.length;
          await page.evaluate(() => {
            const nextBtn = document.querySelector('.ant-pagination-next:not(.ant-pagination-disabled)');
            if (nextBtn) nextBtn.click();
          });
          await new Promise(r => setTimeout(r, 3000));
          if (allLeads.length === before) break; // No new data, stop
        }
      } else {
        // Try direct API calls using the browser's auth context
        console.log("No pagination UI found. Using page.evaluate to fetch remaining pages...");
        let pageNum = 2;
        while (allLeads.length < totalExpected) {
          const moreLeads = await page.evaluate(async (pageNum) => {
            try {
              const res = await fetch(`https://groovio-cms.groovio.com.au/api/v1/studio_leads/?page=${pageNum}`);
              const data = await res.json();
              return data.results || [];
            } catch (e) { return []; }
          }, pageNum);

          if (moreLeads.length === 0) break;
          allLeads.push(...moreLeads);
          console.log(`  Fetched page ${pageNum}: ${moreLeads.length} leads (total: ${allLeads.length}/${totalExpected})`);
          pageNum++;
        }
      }
    }

    console.log(`\nTotal leads captured: ${allLeads.length}`);
    return allLeads;

  } finally {
    await browser.close();
  }
}

if (require.main === module) {
  run().then(leads => {
    console.log(`Done! ${leads.length} leads fetched.`);
    if (leads[0]) console.log("Sample:", JSON.stringify(leads[0], null, 2));
  }).catch(err => {
    console.error("Failed:", err.message);
    process.exit(1);
  });
} else {
  module.exports = { run, STATUS_MAP };
}
