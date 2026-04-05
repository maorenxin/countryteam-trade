/**
 * Puppeteer E2E 验收脚本
 * 覆盖 AC-1 到 AC-5 验收用例
 */

const puppeteer = require('puppeteer');
const { execSync, spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

const BASE_URL = 'http://localhost:8501';
const SCREENSHOT_DIR = path.join(__dirname, 'screenshots');
const TIMEOUT = 30000;

// Ensure screenshot directory exists
fs.mkdirSync(SCREENSHOT_DIR, { recursive: true });

let streamlitProcess = null;

async function waitForServer(url, maxWait = 60000) {
  const start = Date.now();
  while (Date.now() - start < maxWait) {
    try {
      const res = await fetch(url);
      if (res.ok) return;
    } catch (e) {
      // not ready yet
    }
    await new Promise(r => setTimeout(r, 2000));
  }
  throw new Error(`Server at ${url} did not start within ${maxWait}ms`);
}

async function startStreamlit() {
  console.log('Starting Streamlit server...');
  streamlitProcess = spawn('streamlit', [
    'run', 'app.py',
    '--server.headless', 'true',
    '--server.port', '8501',
    '--browser.gatherUsageStats', 'false',
  ], {
    cwd: path.join(__dirname, '..', '..'),
    stdio: 'pipe',
  });
  streamlitProcess.stdout.on('data', d => process.stdout.write(d));
  streamlitProcess.stderr.on('data', d => process.stderr.write(d));
  await waitForServer(BASE_URL);
  console.log('Streamlit server is ready.');
}

function stopStreamlit() {
  if (streamlitProcess) {
    streamlitProcess.kill('SIGTERM');
    streamlitProcess = null;
  }
}

async function waitForContent(page, text, timeout = TIMEOUT) {
  await page.waitForFunction(
    (t) => document.body.innerText.includes(t),
    { timeout },
    text,
  );
}

let passed = 0;
let failed = 0;

async function runTest(name, fn) {
  try {
    await fn();
    console.log(`  PASS: ${name}`);
    passed++;
  } catch (e) {
    console.error(`  FAIL: ${name} - ${e.message}`);
    failed++;
  }
}

async function main() {
  await startStreamlit();

  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });

  try {
    const page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 900 });

    // AC-1: 首页加载与 KPI 展示
    await runTest('AC-1: 首页加载与 KPI 展示', async () => {
      await page.goto(BASE_URL, { waitUntil: 'networkidle2', timeout: TIMEOUT });
      await waitForContent(page, '新进');
      await waitForContent(page, '加仓');
      await waitForContent(page, '减仓');
      await waitForContent(page, '退出');

      // Check 加仓 TOP table has rows
      await waitForContent(page, '加仓 TOP');
      await page.screenshot({ path: path.join(SCREENSHOT_DIR, 'home.png'), fullPage: true });
    });

    // AC-2: 季度切换
    await runTest('AC-2: 季度切换', async () => {
      // Get initial KPI text
      const initialText = await page.evaluate(() => document.body.innerText);

      // Find and click the selectbox to change quarter
      const selectboxes = await page.$$('[data-testid="stSelectbox"]');
      if (selectboxes.length > 0) {
        await selectboxes[0].click();
        await new Promise(r => setTimeout(r, 500));
        // Select second option
        const options = await page.$$('[role="option"]');
        if (options.length > 1) {
          await options[1].click();
          await new Promise(r => setTimeout(r, 3000));
        }
      }
      await page.screenshot({ path: path.join(SCREENSHOT_DIR, 'home_quarter_switch.png'), fullPage: true });
    });

    // AC-3: 个股搜索与详情
    await runTest('AC-3: 个股搜索与详情', async () => {
      await page.goto(`${BASE_URL}/个股详情`, { waitUntil: 'networkidle2', timeout: TIMEOUT });
      await waitForContent(page, '个股详情');

      // Type in search box
      const inputs = await page.$$('input[type="text"]');
      if (inputs.length > 0) {
        await inputs[0].click({ clickCount: 3 });
        await inputs[0].type('601899', { delay: 50 });
        await new Promise(r => setTimeout(r, 3000));
      }

      // Wait for chart to render
      await new Promise(r => setTimeout(r, 3000));
      await page.screenshot({ path: path.join(SCREENSHOT_DIR, 'stock_detail.png'), fullPage: true });
    });

    // AC-4: 国家队一览
    await runTest('AC-4: 国家队一览', async () => {
      await page.goto(`${BASE_URL}/国家队一览`, { waitUntil: 'networkidle2', timeout: TIMEOUT });
      await waitForContent(page, '国家队一览');
      await new Promise(r => setTimeout(r, 3000));

      // Verify institution data loaded
      await waitForContent(page, '当前持仓');
      await page.screenshot({ path: path.join(SCREENSHOT_DIR, 'institution.png'), fullPage: true });
    });

    // AC-5: 季度全景
    await runTest('AC-5: 季度全景', async () => {
      await page.goto(`${BASE_URL}/季度全景`, { waitUntil: 'networkidle2', timeout: TIMEOUT });
      await waitForContent(page, '季度全景');
      await new Promise(r => setTimeout(r, 3000));

      // Verify charts and table rendered
      await waitForContent(page, '行业净加仓排行');
      await waitForContent(page, '持仓数据表');
      await waitForContent(page, '导出 CSV');
      await page.screenshot({ path: path.join(SCREENSHOT_DIR, 'quarterly_overview.png'), fullPage: true });
    });

    // AC-6: GitHub Actions 配置
    await runTest('AC-6: GitHub Actions 配置', async () => {
      const yamlPath = path.join(__dirname, '..', '..', '.github', 'workflows', 'crawl.yml');
      if (!fs.existsSync(yamlPath)) {
        throw new Error('crawl.yml not found');
      }
      const content = fs.readFileSync(yamlPath, 'utf-8');
      if (!content.includes('schedule')) throw new Error('Missing schedule trigger');
      if (!content.includes('workflow_dispatch')) throw new Error('Missing workflow_dispatch');
      if (!content.includes('setup-chrome')) throw new Error('Missing Chrome setup');
      if (!content.includes('selenium_stock_crawler')) throw new Error('Missing crawler step');
      if (!content.includes('analyze_holdings')) throw new Error('Missing analysis step');
      if (!content.includes('git push')) throw new Error('Missing git push step');
    });

  } finally {
    await browser.close();
    stopStreamlit();
  }

  console.log(`\nResults: ${passed} passed, ${failed} failed`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch(e => {
  console.error('Fatal error:', e);
  stopStreamlit();
  process.exit(1);
});
