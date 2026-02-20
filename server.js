console.log('=== YENİ SERVER BAŞLIYOR ===');

require('dotenv').config();

const express = require('express');
const session = require('express-session');
const oracledb = require('oracledb');
const bcrypt = require('bcryptjs');
const { createClient } = require('@supabase/supabase-js');
const path = require('path');

const app = express();
const PORT = 3001;

const oracleClientLibDir = process.env.ORACLE_CLIENT_LIB_DIR || 'C:\\oracle\\instantclient_23_0';
oracledb.initOracleClient({ libDir: oracleClientLibDir });

app.use(express.json());
app.use(express.static('public'));

// Oturum (cookie tabanlı)
const SESSION_SECRET = process.env.SESSION_SECRET || 'dashboard-secret-change-in-production';
app.use(session({
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: { maxAge: 24 * 60 * 60 * 1000, httpOnly: true, sameSite: 'lax' }
}));

// CORS: credentials için origin belirtmek gerekir
app.use((req, res, next) => {
  const origin = req.headers.origin || 'http://localhost:3001';
  res.header('Access-Control-Allow-Origin', origin);
  res.header('Access-Control-Allow-Credentials', 'true');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  next();
});

const dbConfig = {
  user: process.env.ORACLE_USER || 'PROISV',
  password: process.env.ORACLE_PASSWORD || 'PROISV',
  connectString: process.env.ORACLE_CONNECT_STRING || '192.168.8.55:1521/V8'
};

// Supabase: .env'de SUPABASE_URL ve SUPABASE_SERVICE_ROLE_KEY tanımlı olmalı
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const supabase = (supabaseUrl && supabaseServiceKey)
  ? createClient(supabaseUrl, supabaseServiceKey)
  : null;

// Supabase helper: Tabloya veri yaz (DELETE all + INSERT)
async function syncToSupabase(tableName, data, silent = false) {
  if (!supabase || !data || (Array.isArray(data) && data.length === 0)) {
    if (!silent) console.log(`>>> Supabase sync atlandı (${tableName}): veri yok veya supabase yok`);
    return;
  }
  try {
    // Önce tüm eski verileri sil
    const { error: deleteError } = await supabase.from(tableName).delete().neq('id', 0);
    if (deleteError && !silent) console.error(`>>> Supabase DELETE hatası (${tableName}):`, deleteError.message);
    
    // Yeni verileri ekle
    const { error: insertError } = await supabase.from(tableName).insert(data);
    if (insertError) {
      console.error(`>>> Supabase INSERT hatası (${tableName}):`, insertError.message);
      console.error(`>>> Veri örneği:`, JSON.stringify(Array.isArray(data) ? data[0] : data, null, 2));
      return false;
    }
    console.log(`>>> Supabase'e yazıldı: ${tableName} (${Array.isArray(data) ? data.length : 1} satır)`);
    return true;
  } catch (err) {
    console.error(`>>> Supabase sync hatası (${tableName}):`, err.message);
    return false;
  }
}

// Supabase helper: Tek satır upsert (id=1 için)
async function upsertToSupabase(tableName, data, silent = false) {
  if (!supabase || !data) {
    if (!silent) console.log(`>>> Supabase upsert atlandı (${tableName}): veri yok veya supabase yok`);
    return;
  }
  try {
    // Önce tüm satırları sil (tek satır tablolar için)
    await supabase.from(tableName).delete().neq('id', 0);
    // Yeni satırı ekle
    const { error } = await supabase.from(tableName).insert(data);
    if (error) {
      console.error(`>>> Supabase UPSERT hatası (${tableName}):`, error.message);
      console.error(`>>> Veri:`, JSON.stringify(data, null, 2));
      return false;
    }
    console.log(`>>> Supabase'e yazıldı: ${tableName} (upsert)`);
    return true;
  } catch (err) {
    console.error(`>>> Supabase upsert hatası (${tableName}):`, err.message);
    return false;
  }
}

// --- Login / Auth ---
app.post('/api/login', async (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) {
    return res.status(400).json({ success: false, message: 'Kullanıcı adı ve parola gerekli.' });
  }
  if (!supabase) {
    return res.status(503).json({ success: false, message: 'Supabase yapılandırılmamış.' });
  }
  try {
    const { data: user, error } = await supabase
      .from('users')
      .select('id, username, password_hash, display_name, is_active')
      .ilike('username', username)
      .maybeSingle();
    if (error) {
      console.error('>>> Login Supabase hatası:', error.message);
      return res.status(500).json({ success: false, message: 'Giriş kontrolü yapılamadı.' });
    }
    if (!user || !user.is_active) {
      return res.status(401).json({ success: false, message: 'Kullanıcı bulunamadı veya devre dışı.' });
    }
    const match = await bcrypt.compare(password, user.password_hash);
    if (!match) {
      return res.status(401).json({ success: false, message: 'Parola hatalı.' });
    }
    req.session.user = {
      id: user.id,
      username: user.username,
      display_name: user.display_name || user.username
    };
    return res.json({ success: true, display_name: req.session.user.display_name });
  } catch (err) {
    console.error('>>> Login hatası:', err.message);
    return res.status(500).json({ success: false, message: 'Giriş sırasında hata.' });
  }
});

app.get('/api/me', (req, res) => {
  if (req.session && req.session.user) {
    return res.json({ user: req.session.user });
  }
  return res.status(401).json({ message: 'Oturum yok' });
});

app.get('/api/logout', (req, res) => {
  req.session.destroy(() => {});
  res.json({ success: true });
});

// --- Periyodik Sync Fonksiyonları (Oracle → Supabase) ---
const SYNC_INTERVAL_MS = 30 * 60 * 1000; // 30 dakika
const SYNC_START_HOUR = 9;   // 09:00
const SYNC_END_HOUR = 18;    // 17:00 dahil (18'den küçük)

function isWithinSyncHours() {
  const h = new Date().getHours();
  return h >= SYNC_START_HOUR && h < SYNC_END_HOUR;
}

// Sync: Bugünün metrikleri
async function syncTodayMetrics() {
  if (!supabase) return;
  let connection;
  try {
    connection = await oracledb.getConnection(dbConfig);
    const result = await connection.execute(
      `SELECT COUNT(DISTINCT r.reservation_id) AS today_reservations,
              COUNT(*) AS today_rn,
              ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local'
                             THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                             ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END), 0) AS today_revenue
       FROM v8live.pro_isv_reservationinfo_voyage r
       LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
       LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
       WHERE r.reservationstatus = 1 AND TRUNC(r.saledate) = TRUNC(SYSDATE) AND TO_CHAR(r.detail_date, 'YYYY') = '2026'`,
      [], { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );
    const data = result.rows[0] || {};
    await upsertToSupabase('today_metrics', {
      today_reservations: parseInt(data.TODAY_RESERVATIONS || 0),
      today_rn: parseInt(data.TODAY_RN || 0),
      today_revenue: parseFloat(data.TODAY_REVENUE || 0)
    }, true);
    console.log('>>> Sync: today_metrics');
  } catch (err) {
    console.error('>>> Sync hatası (today_metrics):', err.message);
  } finally {
    if (connection) await connection.close();
  }
}

// Sync: Aylık veriler
async function syncMonthlyData() {
  if (!supabase) return;
  let connection;
  try {
    connection = await oracledb.getConnection(dbConfig);
    const result = await connection.execute(
      `WITH m AS (
          SELECT '01' month_num FROM dual UNION ALL SELECT '02' FROM dual UNION ALL SELECT '03' FROM dual UNION ALL
          SELECT '04' FROM dual UNION ALL SELECT '05' FROM dual UNION ALL SELECT '06' FROM dual UNION ALL
          SELECT '07' FROM dual UNION ALL SELECT '08' FROM dual UNION ALL SELECT '09' FROM dual UNION ALL
          SELECT '10' FROM dual UNION ALL SELECT '11' FROM dual UNION ALL SELECT '12' FROM dual
      ),
      d2026 AS (
          SELECT TO_CHAR(r.detail_date, 'MM') AS month_num,
                 COUNT(*) AS total_rn,
                 ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                               ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END), 0) AS total_revenue,
                 ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                               ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END)
                       / NULLIF(SUM(r.noofadults + NVL(r.ca3, 0) / 2), 0), 2) AS avg_rate
          FROM v8live.pro_isv_reservationinfo_voyage r
          LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
          LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
          WHERE r.reservationstatus = 1 AND TO_CHAR(r.detail_date, 'YYYY') = '2026'
            AND r.saledate <= TRUNC(SYSDATE)
          GROUP BY TO_CHAR(r.detail_date, 'MM')
      ),
      d2025 AS (
          SELECT TO_CHAR(r.detail_date, 'MM') AS month_num,
                 COUNT(*) AS total_rn_2025,
                 ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                               ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END), 0) AS total_revenue_2025,
                 ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                               ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END)
                       / NULLIF(SUM(r.noofadults + NVL(r.ca3, 0) / 2), 0), 2) AS avg_rate_2025
          FROM v8live.pro_isv_reservationinfo_voyage r
          LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
          LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
          WHERE r.reservationstatus = 1 AND TO_CHAR(r.detail_date, 'YYYY') = '2025'
            AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -12)
          GROUP BY TO_CHAR(r.detail_date, 'MM')
      ),
      d2024 AS (
          SELECT TO_CHAR(r.detail_date, 'MM') AS month_num,
                 COUNT(*) AS total_rn_2024,
                 ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                               ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END), 0) AS total_revenue_2024,
                 ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                               ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END)
                       / NULLIF(SUM(r.noofadults + NVL(r.ca3, 0) / 2), 0), 2) AS avg_rate_2024
          FROM v8live.pro_isv_reservationinfo_voyage r
          LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
          LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
          WHERE r.reservationstatus = 1 AND TO_CHAR(r.detail_date, 'YYYY') = '2024'
            AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -24)
          GROUP BY TO_CHAR(r.detail_date, 'MM')
      ),
      d2023 AS (
          SELECT TO_CHAR(r.detail_date, 'MM') AS month_num,
                 COUNT(*) AS total_rn_2023,
                 ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                               ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END), 0) AS total_revenue_2023,
                 ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                               ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END)
                       / NULLIF(SUM(r.noofadults + NVL(r.ca3, 0) / 2), 0), 2) AS avg_rate_2023
          FROM v8live.pro_isv_reservationinfo_voyage r
          LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
          LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
          WHERE r.reservationstatus = 1 AND TO_CHAR(r.detail_date, 'YYYY') = '2023'
            AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -36)
          GROUP BY TO_CHAR(r.detail_date, 'MM')
      ),
      d2022 AS (
          SELECT TO_CHAR(r.detail_date, 'MM') AS month_num,
                 COUNT(*) AS total_rn_2022,
                 ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                               ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END), 0) AS total_revenue_2022,
                 ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                               ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END)
                       / NULLIF(SUM(r.noofadults + NVL(r.ca3, 0) / 2), 0), 2) AS avg_rate_2022
          FROM v8live.pro_isv_reservationinfo_voyage r
          LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
          LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
          WHERE r.reservationstatus = 1 AND TO_CHAR(r.detail_date, 'YYYY') = '2022'
            AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -48)
          GROUP BY TO_CHAR(r.detail_date, 'MM')
      )
      SELECT CASE m.month_num WHEN '01' THEN 'Jan' WHEN '02' THEN 'Feb' WHEN '03' THEN 'Mar' WHEN '04' THEN 'Apr'
              WHEN '05' THEN 'May' WHEN '06' THEN 'Jun' WHEN '07' THEN 'Jul' WHEN '08' THEN 'Aug'
              WHEN '09' THEN 'Sep' WHEN '10' THEN 'Oct' WHEN '11' THEN 'Nov' WHEN '12' THEN 'Dec' END AS month,
             m.month_num,
             NVL(d2026.total_rn, 0) AS total_rn,
             NVL(d2026.total_revenue, 0) AS total_revenue,
             NVL(d2026.avg_rate, 0) AS avg_rate,
             NVL(d2025.total_rn_2025, 0) AS total_rn_2025,
             NVL(d2025.total_revenue_2025, 0) AS total_revenue_2025,
             NVL(d2025.avg_rate_2025, 0) AS avg_rate_2025,
             NVL(d2024.total_rn_2024, 0) AS total_rn_2024,
             NVL(d2024.total_revenue_2024, 0) AS total_revenue_2024,
             NVL(d2024.avg_rate_2024, 0) AS avg_rate_2024,
             NVL(d2023.total_rn_2023, 0) AS total_rn_2023,
             NVL(d2023.total_revenue_2023, 0) AS total_revenue_2023,
             NVL(d2023.avg_rate_2023, 0) AS avg_rate_2023,
             NVL(d2022.total_rn_2022, 0) AS total_rn_2022,
             NVL(d2022.total_revenue_2022, 0) AS total_revenue_2022,
             NVL(d2022.avg_rate_2022, 0) AS avg_rate_2022
      FROM m
      LEFT JOIN d2026 ON m.month_num = d2026.month_num
      LEFT JOIN d2025 ON m.month_num = d2025.month_num
      LEFT JOIN d2024 ON m.month_num = d2024.month_num
      LEFT JOIN d2023 ON m.month_num = d2023.month_num
      LEFT JOIN d2022 ON m.month_num = d2022.month_num
      ORDER BY m.month_num`,
      [], { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );
    const supabaseData = (result.rows || []).map(row => ({
      month_num: row.MONTH_NUM || '',
      month_label: row.MONTH || '',
      total_rn: parseInt(row.TOTAL_RN || 0),
      total_revenue: parseFloat(row.TOTAL_REVENUE || 0),
      avg_rate: parseFloat(row.AVG_RATE || 0),
      total_rn_2025: parseInt(row.TOTAL_RN_2025 || 0),
      total_revenue_2025: parseFloat(row.TOTAL_REVENUE_2025 || 0),
      avg_rate_2025: parseFloat(row.AVG_RATE_2025 || 0),
      total_rn_2024: parseInt(row.TOTAL_RN_2024 || 0),
      total_revenue_2024: parseFloat(row.TOTAL_REVENUE_2024 || 0),
      avg_rate_2024: parseFloat(row.AVG_RATE_2024 || 0),
      total_rn_2023: parseInt(row.TOTAL_RN_2023 || 0),
      total_revenue_2023: parseFloat(row.TOTAL_REVENUE_2023 || 0),
      avg_rate_2023: parseFloat(row.AVG_RATE_2023 || 0),
      total_rn_2022: parseInt(row.TOTAL_RN_2022 || 0),
      total_revenue_2022: parseFloat(row.TOTAL_REVENUE_2022 || 0),
      avg_rate_2022: parseFloat(row.AVG_RATE_2022 || 0)
    }));
    await syncToSupabase('monthly_data', supabaseData, true);
    console.log('>>> Sync: monthly_data');
  } catch (err) {
    console.error('>>> Sync hatası (monthly_data):', err.message);
  } finally {
    if (connection) await connection.close();
  }
}

// Sync: RN Heatmap (Oracle → Supabase rn_heatmap + rn_heatmap_meta)
async function syncRnHeatmap() {
  if (!supabase) return;
  let connection;
  function parseRnAdb(val) {
    if (val == null || typeof val !== 'string') return null;
    const parts = val.trim().split(/\s*\/\s*/);
    if (parts.length < 2) return null;
    const rn = parseFloat(parts[0].replace(/,/g, '').trim());
    const price = parseFloat(parts[1].replace(/,/g, '').trim());
    if (isNaN(rn) || isNaN(price)) return null;
    return { rn, price };
  }
  try {
    connection = await oracledb.getConnection(dbConfig);
    const result = await connection.execute(
      `SELECT * FROM (
          SELECT
              NVL(Stay_Ay, 'TOPLAM') AS Stay_Ay,
              NVL(Pazar, 'GENEL TOPLAM') AS Pazar,
              Oda_Tipi,
              TO_CHAR(SUM(RN_ODA), 'FM999,990') || ' / ' ||
              TO_CHAR(ROUND(SUM(Rev) / NULLIF(SUM(PAX_HESABI), 0), 2), 'FM999,990.00') AS RN_ADB
          FROM (
              SELECT
                  TO_CHAR(r.detail_date, 'YYYY-MM') AS Stay_Ay,
                  r.mainmarketcode_long AS Pazar,
                  r.rateroomtype AS Oda_Tipi,
                  1 AS RN_ODA,
                  (r.noofadults + NVL(r.ca3, 0) / 2) AS PAX_HESABI,
                  CASE WHEN r.mainmarketcode_long = 'Local'
                       THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                       ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0)
                  END AS Rev
              FROM v8live.pro_isv_reservationinfo_voyage r
              LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
              LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
              WHERE r.reservationstatus = 1
                AND r.detail_date >= TRUNC(SYSDATE)
                AND TO_CHAR(r.detail_date, 'YYYY') IN ('2025', '2026')
              UNION ALL
              SELECT
                  TO_CHAR(r.detail_date, 'YYYY-MM') AS Stay_Ay,
                  r.mainmarketcode_long AS Pazar,
                  'TUM_ODALAR' AS Oda_Tipi,
                  1 AS RN_ODA,
                  (r.noofadults + NVL(r.ca3, 0) / 2) AS PAX_HESABI,
                  CASE WHEN r.mainmarketcode_long = 'Local'
                       THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                       ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0)
                  END AS Rev
              FROM v8live.pro_isv_reservationinfo_voyage r
              LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
              LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
              WHERE r.reservationstatus = 1
                AND r.detail_date >= TRUNC(SYSDATE)
                AND TO_CHAR(r.detail_date, 'YYYY') IN ('2025', '2026')
          )
          GROUP BY ROLLUP(Stay_Ay, Pazar), Oda_Tipi
      )
      PIVOT (
          MAX(RN_ADB)
          FOR Oda_Tipi IN (
              'VILLA' AS "BUNGALOV_236",
              'OTEL'  AS "STANDART_LAND_VIEW_57",
              'ODNZ'  AS "STANDART_SEA_VIEW_70",
              'FAM'   AS "BUNGALOV_AILE_133",
              'OFAM'  AS "STANDART_FAMILY_8",
              'SUIT'  AS "SUITE_4",
              'TUM_ODALAR' AS "TOPLAM_508"
          )
      )
      ORDER BY
          CASE WHEN Stay_Ay = 'TOPLAM' THEN 2 ELSE 1 END,
          Stay_Ay,
          CASE WHEN Pazar = 'GENEL TOPLAM' THEN 2 ELSE 1 END,
          Pazar`,
      [],
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );
    const rows = result.rows || [];
    const colMap = [
      { key: 'BUNGALOV_236', id: 'BUNGALOV' },
      { key: 'STANDART_LAND_VIEW_57', id: 'STANDART_LAND_VIEW' },
      { key: 'STANDART_SEA_VIEW_70', id: 'STANDART_SEA_VIEW' },
      { key: 'BUNGALOV_AILE_133', id: 'BUNGALOV_AILE_ODASI' },
      { key: 'STANDART_FAMILY_8', id: 'STANDART_FAMILY_ROOM' },
      { key: 'SUITE_4', id: 'SUITE' },
      { key: 'TOPLAM_508', id: 'TUM_ODALAR' }
    ];
    const out = [];
    let yearTotalRn = null;
    const lastColKey = 'TOPLAM_508';
    rows.forEach((row) => {
      const stayAy = row.STAY_AY || row.Stay_Ay || row.stay_ay;
      const pazar = row.PAZAR || row.Pazar || row.pazar;
      if (!stayAy || !pazar) return;
      if (stayAy === 'TOPLAM' && pazar === 'GENEL TOPLAM') {
        const val = row[lastColKey] != null ? row[lastColKey] : (row[lastColKey.toUpperCase()]);
        const parsed = parseRnAdb(val);
        if (parsed && !isNaN(parsed.rn)) yearTotalRn = Math.round(parsed.rn);
        return;
      }
      colMap.forEach(({ key, id }) => {
        const val = row[key] != null ? row[key] : (row[key.toUpperCase()]);
        const parsed = parseRnAdb(val);
        if (parsed && (parsed.rn > 0 || parsed.price > 0)) {
          out.push({
            month_key: stayAy,
            market: pazar,
            room_type: id,
            rn: parsed.rn,
            price: parsed.price
          });
        }
      });
    });
    if (out.length > 0) await syncToSupabase('rn_heatmap', out, true);
    if (yearTotalRn != null) await syncToSupabase('rn_heatmap_meta', [{ key: 'year_total_rn', value: yearTotalRn }], true);
    console.log('>>> Sync: rn_heatmap');
  } catch (err) {
    console.error('>>> Sync hatası (rn_heatmap):', err.message);
  } finally {
    if (connection) await connection.close();
  }
}

// Sync: ALOS & ADB Heatmap (Oracle → Supabase alos_adb_heatmap)
async function syncAlosAdbHeatmap() {
  if (!supabase) return;
  let connection;
  try {
    connection = await oracledb.getConnection(dbConfig);
    const result = await connection.execute(
      `SELECT * FROM (
    SELECT
        TO_CHAR(r.detail_date, 'YYYY-MM') AS Ay,
        r.mainmarketcode_long AS Pazar,
        ROUND(AVG(r.departuredate - r.arrivaldate), 1) || ' ; ' ||
        ROUND(
            SUM(CASE WHEN r.mainmarketcode_long = 'Local'
                     THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                     ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END)
            / NULLIF(SUM(r.departuredate - r.arrivaldate), 0), 2
        ) AS ALOS_ADB
    FROM
        v8live.pro_isv_reservationinfo_voyage r
        LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
        LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
    WHERE
        r.reservationstatus = 1
        AND (r.departuredate - r.arrivaldate) > 0
        AND (
            (TO_CHAR(r.detail_date, 'YYYY') = '2025' AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -12))
            OR
            (TO_CHAR(r.detail_date, 'YYYY') = '2026' AND r.saledate <= TRUNC(SYSDATE))
        )
    GROUP BY
        TO_CHAR(r.detail_date, 'YYYY-MM'),
        r.mainmarketcode_long
)
PIVOT (
    MAX(ALOS_ADB)
    FOR Ay IN (
        '2025-01' AS JAN_25, '2025-02' AS FEB_25, '2025-03' AS MAR_25, '2025-04' AS APR_25,
        '2025-05' AS MAY_25, '2025-06' AS JUN_25, '2025-07' AS JUL_25, '2025-08' AS AUG_25,
        '2025-09' AS SEP_25, '2025-10' AS OCT_25, '2025-11' AS NOV_25, '2025-12' AS DEC_25,
        '2026-01' AS JAN_26, '2026-02' AS FEB_26, '2026-03' AS MAR_26, '2026-04' AS APR_26,
        '2026-05' AS MAY_26, '2026-06' AS JUN_26, '2026-07' AS JUL_26, '2026-08' AS AUG_26,
        '2026-09' AS SEP_26, '2026-10' AS OCT_26, '2026-11' AS NOV_26, '2026-12' AS DEC_26
    )
)
ORDER BY Pazar`,
      [],
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );
    const rows = result.rows || [];
    if (rows.length > 0) await syncToSupabase('alos_adb_heatmap', [{ data: rows }], true);
    console.log('>>> Sync: alos_adb_heatmap');
  } catch (err) {
    console.error('>>> Sync hatası (alos_adb_heatmap):', err.message);
  } finally {
    if (connection) await connection.close();
  }
}

// Sync: BOB Revenue Analysis (Oracle → Supabase bob_revenue_analysis)
async function syncBobRevenueAnalysis() {
  if (!supabase) return;
  let connection;
  try {
    connection = await oracledb.getConnection(dbConfig);
    const result = await connection.execute(
      `SELECT
          TO_CHAR(r.detail_date, 'MM') AS month_num,
          r.mainmarketcode_long AS market,
          TO_NUMBER(TO_CHAR(r.detail_date, 'YYYY')) AS year,
          SUM(CASE WHEN r.mainmarketcode_long = 'Local'
                   THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                   ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END) AS bob_revenue,
          SUM(r.noofadults + NVL(r.ca3, 0) / 2) AS bob_pax,
          COUNT(*) AS bob_rn
       FROM v8live.pro_isv_reservationinfo_voyage r
       LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
       LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
       WHERE r.reservationstatus = 1
         AND TO_CHAR(r.detail_date, 'YYYY') IN ('2022','2023','2024','2025','2026')
         AND r.saledate <= ADD_MONTHS(
               TRUNC(SYSDATE),
               -12 * (2026 - TO_NUMBER(TO_CHAR(r.detail_date, 'YYYY')))
             )
       GROUP BY TO_CHAR(r.detail_date, 'MM'), r.mainmarketcode_long, TO_NUMBER(TO_CHAR(r.detail_date, 'YYYY'))`,
      [],
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );
    const rows = result.rows || [];
    const supabaseData = rows.map(row => ({
      month_num: String(row.MONTH_NUM ?? row.month_num ?? '').replace(/^(\d)$/, '0$1'),
      market: row.MARKET ?? row.market ?? null,
      year: parseInt(row.YEAR ?? row.year ?? 0, 10),
      bob_revenue: parseFloat(row.BOB_REVENUE ?? row.bob_revenue ?? 0),
      bob_pax: parseInt(row.BOB_PAX ?? row.bob_pax ?? 0, 10),
      bob_rn: parseInt(row.BOB_RN ?? row.bob_rn ?? 0, 10)
    })).filter(r => r.month_num && r.year >= 2022 && r.year <= 2026);
    if (supabaseData.length > 0) await syncToSupabase('bob_revenue_analysis', supabaseData, true);
    console.log('>>> Sync: bob_revenue_analysis');
  } catch (err) {
    console.error('>>> Sync hatası (bob_revenue_analysis):', err.message);
  } finally {
    if (connection) await connection.close();
  }
}

// Sync: Bugün acente bazlı RN
async function syncTodayAgentRn() {
  let connection;
  try {
    connection = await oracledb.getConnection(dbConfig);
    const result = await connection.execute(
      `SELECT r.AGENTNAME AS segment, COUNT(*) AS rn_count,
              ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0) ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END), 0) AS revenue
       FROM v8live.pro_isv_reservationinfo_voyage r
       LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
       LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
       WHERE r.reservationstatus = 1 AND TRUNC(r.saledate) = TRUNC(SYSDATE) AND TO_CHAR(r.detail_date, 'YYYY') = '2026'
       GROUP BY r.AGENTNAME ORDER BY rn_count DESC`,
      [], { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );
    const data = result.rows || [];
    if (supabase && data.length > 0) {
      const supabaseData = data.map(row => ({
        segment: row.SEGMENT || row.segment || '',
        rn_count: parseInt(row.RN_COUNT || row.rn_count || 0),
        revenue: parseFloat(row.REVENUE || row.revenue || 0)
      }));
      await syncToSupabase('today_agent_rn', supabaseData, true);
    }
    console.log('>>> Sync: today_agent_rn');
    return data;
  } catch (err) {
    console.error('>>> Sync hatası (today_agent_rn):', err.message);
    return [];
  } finally {
    if (connection) await connection.close();
  }
}

// Sync: Bugün girilen RN aylık dağılım
async function syncTodayRnByMonth() {
  let connection;
  try {
    connection = await oracledb.getConnection(dbConfig);
    const result = await connection.execute(
      `SELECT TO_CHAR(r.detail_date, 'MM') AS month_num, COUNT(*) AS total_rn,
              ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0) ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END), 0) AS total_revenue,
              ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0) ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END) / NULLIF(SUM(r.noofadults + NVL(r.ca3, 0) / 2), 0), 2) AS adb
       FROM v8live.pro_isv_reservationinfo_voyage r
       LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
       LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
       WHERE r.reservationstatus = 1 AND TRUNC(r.saledate) = TRUNC(SYSDATE) AND TO_CHAR(r.detail_date, 'YYYY') = '2026'
       GROUP BY TO_CHAR(r.detail_date, 'MM') ORDER BY TO_CHAR(r.detail_date, 'MM')`,
      [], { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );
    const data = result.rows || [];
    if (supabase && data.length > 0) {
      const supabaseData = data.map(row => ({
        month_num: row.MONTH_NUM || row.month_num || '',
        total_rn: parseInt(row.TOTAL_RN || row.total_rn || 0),
        total_revenue: parseFloat(row.TOTAL_REVENUE || row.total_revenue || 0),
        adb: parseFloat(row.ADB || row.adb || 0)
      }));
      await syncToSupabase('today_rn_by_month', supabaseData, true);
    }
    console.log('>>> Sync: today_rn_by_month');
    return data;
  } catch (err) {
    console.error('>>> Sync hatası (today_rn_by_month):', err.message);
    return [];
  } finally {
    if (connection) await connection.close();
  }
}

// Sync: Bugün girilen RN aylık pazar dağılımı
async function syncTodayRnByMonthMarket() {
  let connection;
  try {
    connection = await oracledb.getConnection(dbConfig);
    const result = await connection.execute(
      `WITH market_totals AS (
          SELECT r.mainmarketcode_long, COUNT(*) AS total_rn FROM v8live.pro_isv_reservationinfo_voyage r
          WHERE r.reservationstatus = 1 AND TO_CHAR(r.detail_date, 'YYYY') = '2026' AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -12 * (2026 - TO_NUMBER(TO_CHAR(r.detail_date, 'YYYY'))))
          GROUP BY r.mainmarketcode_long ORDER BY total_rn DESC FETCH FIRST 15 ROWS ONLY
      ),
      today_by_month_market AS (
          SELECT TO_CHAR(r.detail_date, 'MM') AS month_num, r.mainmarketcode_long AS market, COUNT(*) AS rn
          FROM v8live.pro_isv_reservationinfo_voyage r
          WHERE r.reservationstatus = 1 AND TRUNC(r.saledate) = TRUNC(SYSDATE) AND TO_CHAR(r.detail_date, 'YYYY') = '2026'
            AND r.mainmarketcode_long IN (SELECT mainmarketcode_long FROM market_totals)
          GROUP BY TO_CHAR(r.detail_date, 'MM'), r.mainmarketcode_long
      )
      SELECT t.month_num, t.market, t.rn, mt.total_rn AS market_total FROM today_by_month_market t
      JOIN market_totals mt ON t.market = mt.mainmarketcode_long ORDER BY t.month_num, mt.total_rn DESC`,
      [], { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );
    const data = result.rows || [];
    if (supabase && data.length > 0) {
      const supabaseData = data.map(row => ({
        month_num: row.MONTH_NUM || row.month_num || '',
        market: row.MARKET || row.market || '',
        rn: parseInt(row.RN || row.rn || 0),
        market_total: parseInt(row.MARKET_TOTAL || row.market_total || 0)
      }));
      await syncToSupabase('today_rn_by_month_market', supabaseData, true);
    }
    console.log('>>> Sync: today_rn_by_month_market');
    return data;
  } catch (err) {
    console.error('>>> Sync hatası (today_rn_by_month_market):', err.message);
    return [];
  } finally {
    if (connection) await connection.close();
  }
}

// Sync: Günlük pazar bazlı RN (2026 + totals 2025-2022)
async function syncDailyMarketRn() {
  let connection;
  try {
    connection = await oracledb.getConnection(dbConfig);
    const result = await connection.execute(
      `WITH market_totals AS (
          SELECT r.mainmarketcode_long, COUNT(*) as total_rn FROM v8live.pro_isv_reservationinfo_voyage r
          WHERE r.reservationstatus = 1 AND TO_CHAR(r.detail_date, 'YYYY') = '2026' AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -12 * (2026 - TO_NUMBER(TO_CHAR(r.detail_date, 'YYYY'))))
          GROUP BY r.mainmarketcode_long ORDER BY total_rn DESC FETCH FIRST 15 ROWS ONLY
      ),
      daily_data AS (
          SELECT r.detail_date, r.mainmarketcode_long, COUNT(*) as rn_count FROM v8live.pro_isv_reservationinfo_voyage r
          WHERE r.reservationstatus = 1 AND TO_CHAR(r.detail_date, 'YYYY') = '2026' AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -12 * (2026 - TO_NUMBER(TO_CHAR(r.detail_date, 'YYYY'))))
            AND r.mainmarketcode_long IN (SELECT mainmarketcode_long FROM market_totals)
          GROUP BY r.detail_date, r.mainmarketcode_long
      )
      SELECT TO_CHAR(dd.detail_date, 'YYYY-MM-DD') as date_str, dd.mainmarketcode_long as market, dd.rn_count, mt.total_rn as market_total
      FROM daily_data dd JOIN market_totals mt ON dd.mainmarketcode_long = mt.mainmarketcode_long ORDER BY dd.detail_date, mt.total_rn DESC`,
      [], { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );
    const topMarkets = [...new Set((result.rows || []).map(r => (r.MARKET || r.market || '').trim()).filter(Boolean))];
    const inClause = topMarkets.map((_, i) => ':' + (i + 1)).join(',');
    const inSql = topMarkets.length ? ` AND r.mainmarketcode_long IN (${inClause})` : '';
    const bindArr = topMarkets.length ? topMarkets : [];
    const [result2025, result2024, result2023, result2022] = await Promise.all([
      connection.execute(`SELECT TO_CHAR(r.detail_date, 'MM-DD') AS month_day, SUM(1) AS total_rn FROM v8live.pro_isv_reservationinfo_voyage r WHERE r.reservationstatus = 1 AND TO_CHAR(r.detail_date, 'YYYY') = '2025' AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -12)${inSql} GROUP BY TO_CHAR(r.detail_date, 'MM-DD') ORDER BY TO_CHAR(r.detail_date, 'MM-DD')`, bindArr, { outFormat: oracledb.OUT_FORMAT_OBJECT }),
      connection.execute(`SELECT TO_CHAR(r.detail_date, 'MM-DD') AS month_day, SUM(1) AS total_rn FROM v8live.pro_isv_reservationinfo_voyage r WHERE r.reservationstatus = 1 AND TO_CHAR(r.detail_date, 'YYYY') = '2024' AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -24)${inSql} GROUP BY TO_CHAR(r.detail_date, 'MM-DD') ORDER BY TO_CHAR(r.detail_date, 'MM-DD')`, bindArr, { outFormat: oracledb.OUT_FORMAT_OBJECT }),
      connection.execute(`SELECT TO_CHAR(r.detail_date, 'MM-DD') AS month_day, SUM(1) AS total_rn FROM v8live.pro_isv_reservationinfo_voyage r WHERE r.reservationstatus = 1 AND TO_CHAR(r.detail_date, 'YYYY') = '2023' AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -36)${inSql} GROUP BY TO_CHAR(r.detail_date, 'MM-DD') ORDER BY TO_CHAR(r.detail_date, 'MM-DD')`, bindArr, { outFormat: oracledb.OUT_FORMAT_OBJECT }),
      connection.execute(`SELECT TO_CHAR(r.detail_date, 'MM-DD') AS month_day, SUM(1) AS total_rn FROM v8live.pro_isv_reservationinfo_voyage r WHERE r.reservationstatus = 1 AND TO_CHAR(r.detail_date, 'YYYY') = '2022' AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -48)${inSql} GROUP BY TO_CHAR(r.detail_date, 'MM-DD') ORDER BY TO_CHAR(r.detail_date, 'MM-DD')`, bindArr, { outFormat: oracledb.OUT_FORMAT_OBJECT })
    ]);
    if (supabase && result.rows && result.rows.length > 0) {
      const supabaseData2026 = result.rows.map(row => ({
        date_str: row.DATE_STR || row.date_str || '',
        market: row.MARKET || row.market || '',
        rn_count: parseInt(row.RN_COUNT || row.rn_count || 0),
        market_total: parseInt(row.MARKET_TOTAL || row.market_total || 0)
      }));
      await syncToSupabase('daily_market_rn', supabaseData2026, true);
    }
    const totalsData = [];
    [result2025.rows, result2024.rows, result2023.rows, result2022.rows].forEach((rows, idx) => {
      const year = 2025 - idx;
      if (rows && rows.length > 0) rows.forEach(row => totalsData.push({ year_num: year, month_day: row.MONTH_DAY || row.month_day || '', total_rn: parseInt(row.TOTAL_RN || row.total_rn || 0) }));
    });
    if (supabase && totalsData.length > 0) await syncToSupabase('daily_market_rn_totals', totalsData, true);
    console.log('>>> Sync: daily_market_rn, daily_market_rn_totals');
    return {
      data2026: result.rows,
      daily_2025_totals: result2025.rows,
      daily_2024_totals: result2024.rows,
      daily_2023_totals: result2023.rows,
      daily_2022_totals: result2022.rows
    };
  } catch (err) {
    console.error('>>> Sync hatası (daily_market_rn):', err.message);
    return { data2026: [], daily_2025_totals: [], daily_2024_totals: [], daily_2023_totals: [], daily_2022_totals: [] };
  } finally {
    if (connection) await connection.close();
  }
}

// Sync: Booking pace
async function syncBookingPace() {
  let connection;
  try {
    connection = await oracledb.getConnection(dbConfig);
    const result = await connection.execute(
      `SELECT CASE t_m.month_num WHEN '01' THEN 'Jan' WHEN '02' THEN 'Feb' WHEN '03' THEN 'Mar' WHEN '04' THEN 'Apr' WHEN '05' THEN 'May' WHEN '06' THEN 'Jun' WHEN '07' THEN 'Jul' WHEN '08' THEN 'Aug' WHEN '09' THEN 'Sep' WHEN '10' THEN 'Oct' WHEN '11' THEN 'Nov' WHEN '12' THEN 'Dec' END AS month, t_m.month_num,
          NVL(last_30_2026.rn, 0) AS last_30_days_rn, NVL(last_15_2026.rn, 0) AS last_15_days_rn,
          NVL(last_30_2025.rn, 0) AS last_30_days_2025_rn, NVL(last_15_2025.rn, 0) AS last_15_days_2025_rn
      FROM (SELECT '01' month_num FROM dual UNION ALL SELECT '02' FROM dual UNION ALL SELECT '03' FROM dual UNION ALL SELECT '04' FROM dual UNION ALL SELECT '05' FROM dual UNION ALL SELECT '06' FROM dual UNION ALL SELECT '07' FROM dual UNION ALL SELECT '08' FROM dual UNION ALL SELECT '09' FROM dual UNION ALL SELECT '10' FROM dual UNION ALL SELECT '11' FROM dual UNION ALL SELECT '12' FROM dual) t_m
      LEFT JOIN (SELECT TO_CHAR(r.detail_date, 'MM') AS month_num, COUNT(*) AS rn FROM v8live.pro_isv_reservationinfo_voyage r WHERE r.reservationstatus = 1 AND TO_CHAR(r.detail_date, 'YYYY') = '2026' AND r.saledate > TRUNC(SYSDATE) - 30 AND r.saledate <= TRUNC(SYSDATE) - 15 GROUP BY TO_CHAR(r.detail_date, 'MM')) last_30_2026 ON t_m.month_num = last_30_2026.month_num
      LEFT JOIN (SELECT TO_CHAR(r.detail_date, 'MM') AS month_num, COUNT(*) AS rn FROM v8live.pro_isv_reservationinfo_voyage r WHERE r.reservationstatus = 1 AND TO_CHAR(r.detail_date, 'YYYY') = '2026' AND r.saledate > TRUNC(SYSDATE) - 15 GROUP BY TO_CHAR(r.detail_date, 'MM')) last_15_2026 ON t_m.month_num = last_15_2026.month_num
      LEFT JOIN (SELECT TO_CHAR(r.detail_date, 'MM') AS month_num, COUNT(*) AS rn FROM v8live.pro_isv_reservationinfo_voyage r WHERE r.reservationstatus = 1 AND TO_CHAR(r.detail_date, 'YYYY') = '2025' AND r.saledate > ADD_MONTHS(TRUNC(SYSDATE), -12) - 30 AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -12) - 15 GROUP BY TO_CHAR(r.detail_date, 'MM')) last_30_2025 ON t_m.month_num = last_30_2025.month_num
      LEFT JOIN (SELECT TO_CHAR(r.detail_date, 'MM') AS month_num, COUNT(*) AS rn FROM v8live.pro_isv_reservationinfo_voyage r WHERE r.reservationstatus = 1 AND TO_CHAR(r.detail_date, 'YYYY') = '2025' AND r.saledate > ADD_MONTHS(TRUNC(SYSDATE), -12) - 15 AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -12) GROUP BY TO_CHAR(r.detail_date, 'MM')) last_15_2025 ON t_m.month_num = last_15_2025.month_num
      ORDER BY t_m.month_num`,
      [], { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );
    const data = result.rows || [];
    if (supabase && data.length > 0) {
      const supabaseData = data.map(row => ({
        month_num: row.MONTH_NUM || row.month_num || '',
        month_label: row.MONTH || row.month || '',
        last_30_days_rn: parseInt(row.LAST_30_DAYS_RN || row.last_30_days_rn || 0),
        last_15_days_rn: parseInt(row.LAST_15_DAYS_RN || row.last_15_days_rn || 0),
        last_30_days_2025_rn: parseInt(row.LAST_30_DAYS_2025_RN || row.last_30_days_2025_rn || 0),
        last_15_days_2025_rn: parseInt(row.LAST_15_DAYS_2025_RN || row.last_15_days_2025_rn || 0)
      }));
      await syncToSupabase('booking_pace', supabaseData, true);
    }
    console.log('>>> Sync: booking_pace');
    return data;
  } catch (err) {
    console.error('>>> Sync hatası (booking_pace):', err.message);
    return [];
  } finally {
    if (connection) await connection.close();
  }
}

// Sync: Yıllık gelir hedefi (annual_target)
async function syncAnnualTarget() {
  let connection;
  try {
    connection = await oracledb.getConnection(dbConfig);
    const result = await connection.execute(
      `SELECT ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0) ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END), 0) AS total_revenue_2026
       FROM v8live.pro_isv_reservationinfo_voyage r
       LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
       LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
       WHERE r.reservationstatus = 1 AND TO_CHAR(r.detail_date, 'YYYY') = '2026' AND r.saledate <= TRUNC(SYSDATE)`,
      [], { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );
    const row = result.rows && result.rows[0] ? result.rows[0] : {};
    const totalRevenue = parseFloat(row.TOTAL_REVENUE_2026 || 0);
    if (supabase) await upsertToSupabase('annual_target', { total_revenue_2026: totalRevenue }, true);
    console.log('>>> Sync: annual_target');
    return { TOTAL_REVENUE_2026: totalRevenue };
  } catch (err) {
    console.error('>>> Sync hatası (annual_target):', err.message);
    return { TOTAL_REVENUE_2026: 0 };
  } finally {
    if (connection) await connection.close();
  }
}

// Sync: Acente performansı (agent_performance)
async function syncAgentPerformance() {
  let connection;
  try {
    connection = await oracledb.getConnection(dbConfig);
    const result = await connection.execute(
      `WITH detail AS (
          SELECT r.AGENTNAME, TO_CHAR(r.detail_date, 'YYYY') AS year_num,
                 CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0) ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END AS revenue,
                 NVL(TRIM(r.mainmarketcode_long), 'Other') AS market
          FROM v8live.pro_isv_reservationinfo_voyage r
          LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
          LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
          WHERE r.reservationstatus = 1 AND r.AGENTNAME IS NOT NULL AND TO_CHAR(r.detail_date, 'YYYY') IN ('2025', '2026')
            AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -12 * (TO_NUMBER('2026') - TO_NUMBER(TO_CHAR(r.detail_date, 'YYYY'))))
      ),
      totals AS (SELECT AGENTNAME, SUM(CASE WHEN year_num = '2026' THEN revenue END) AS rev FROM detail GROUP BY AGENTNAME),
      ranked AS (SELECT AGENTNAME, ROW_NUMBER() OVER (ORDER BY rev DESC NULLS LAST) AS rn FROM totals),
      top_20 AS (SELECT AGENTNAME, rn FROM ranked WHERE rn <= 20),
      agg AS (
          SELECT r.AGENTNAME AS SEGMENT, r.market AS MARKET, t.rn AS AGENT_ORDER,
                 ROUND(SUM(CASE WHEN r.year_num = '2026' THEN r.revenue END), 0) AS REVENUE_2026,
                 ROUND(SUM(CASE WHEN r.year_num = '2025' THEN r.revenue END), 0) AS REVENUE_2025
          FROM detail r JOIN top_20 t ON r.AGENTNAME = t.AGENTNAME
          GROUP BY r.AGENTNAME, r.market, t.rn
      )
      SELECT SEGMENT, MARKET, REVENUE_2026, REVENUE_2025, AGENT_ORDER FROM agg ORDER BY AGENT_ORDER, REVENUE_2026 DESC`,
      [], { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );
    const data = result.rows || [];
    if (supabase && data.length > 0) {
      const supabaseData = data.map(row => ({
        segment: row.SEGMENT || row.segment || '',
        market: row.MARKET || row.market || '',
        revenue_2026: parseFloat(row.REVENUE_2026 || row.revenue_2026 || 0),
        revenue_2025: parseFloat(row.REVENUE_2025 || row.revenue_2025 || 0),
        agent_order: parseInt(row.AGENT_ORDER || row.agent_order || 0)
      }));
      await syncToSupabase('agent_performance', supabaseData, true);
    }
    console.log('>>> Sync: agent_performance');
    return data;
  } catch (err) {
    console.error('>>> Sync hatası (agent_performance):', err.message);
    return [];
  } finally {
    if (connection) await connection.close();
  }
}

// Sync: Mainmarket BOB (market_mainmarket)
async function syncMarketMainmarket() {
  let connection;
  try {
    connection = await oracledb.getConnection(dbConfig);
    const result = await connection.execute(
      `WITH t_p AS (
          SELECT r.mainmarketcode_long, TO_CHAR(r.detail_date, 'YYYY') AS year_num,
                 CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0) ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END AS revenue,
                 r.noofadults + NVL(r.ca3, 0) / 2 AS pax, 1 AS room_nights
          FROM v8live.pro_isv_reservationinfo_voyage r
          LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
          LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
          WHERE r.reservationstatus = 1 AND r.mainmarketcode_long IS NOT NULL AND TO_CHAR(r.detail_date, 'YYYY') IN ('2022', '2023', '2024', '2025', '2026')
            AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -12 * (TO_NUMBER('2026') - TO_NUMBER(TO_CHAR(r.detail_date, 'YYYY'))))
      ),
      t_b AS (SELECT 'Local' AS market_code, 14925123 AS bud_revenue, 85668 AS bud_pax, 174.22 AS bud_adb FROM dual UNION ALL SELECT 'Great Britain', 3061206, 19271, 158.85 FROM dual UNION ALL SELECT 'Russia', 9179980, 50479, 181.86 FROM dual UNION ALL SELECT 'West Europe', 13452128, 86368, 155.75 FROM dual UNION ALL SELECT 'CIS (BDT)', 0, 1, 1 FROM dual UNION ALL SELECT 'Baltic', 0, 1, 1 FROM dual UNION ALL SELECT 'East Europe', 0, 1, 1 FROM dual UNION ALL SELECT 'Scandinavian', 0, 1, 1 FROM dual)
      SELECT t_p.mainmarketcode_long AS SEGMENT,
          ROUND(SUM(CASE WHEN t_p.year_num = '2026' THEN t_p.revenue END), 0) AS REVENUE_2026,
          ROUND(SUM(CASE WHEN t_p.year_num = '2025' THEN t_p.revenue END), 0) AS REVENUE_2025,
          ROUND(SUM(CASE WHEN t_p.year_num = '2024' THEN t_p.revenue END), 0) AS REVENUE_2024,
          ROUND(SUM(CASE WHEN t_p.year_num = '2023' THEN t_p.revenue END), 0) AS REVENUE_2023,
          ROUND(SUM(CASE WHEN t_p.year_num = '2022' THEN t_p.revenue END), 0) AS REVENUE_2022,
          SUM(CASE WHEN t_p.year_num = '2026' THEN t_p.room_nights END) AS RN_2026, SUM(CASE WHEN t_p.year_num = '2025' THEN t_p.room_nights END) AS RN_2025,
          SUM(CASE WHEN t_p.year_num = '2024' THEN t_p.room_nights END) AS RN_2024, SUM(CASE WHEN t_p.year_num = '2023' THEN t_p.room_nights END) AS RN_2023, SUM(CASE WHEN t_p.year_num = '2022' THEN t_p.room_nights END) AS RN_2022
      FROM t_p LEFT JOIN t_b ON t_p.mainmarketcode_long = t_b.market_code GROUP BY t_p.mainmarketcode_long ORDER BY t_p.mainmarketcode_long`,
      [], { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );
    const data = result.rows || [];
    if (supabase && data.length > 0) {
      const supabaseData = data.map(row => ({
        segment: row.SEGMENT || row.segment || '',
        revenue_2026: parseFloat(row.REVENUE_2026 || row.revenue_2026 || 0),
        revenue_2025: parseFloat(row.REVENUE_2025 || row.revenue_2025 || 0),
        revenue_2024: parseFloat(row.REVENUE_2024 || row.revenue_2024 || 0),
        revenue_2023: parseFloat(row.REVENUE_2023 || row.revenue_2023 || 0),
        revenue_2022: parseFloat(row.REVENUE_2022 || row.revenue_2022 || 0),
        rn_2026: parseInt(row.RN_2026 || row.rn_2026 || 0),
        rn_2025: parseInt(row.RN_2025 || row.rn_2025 || 0),
        rn_2024: parseInt(row.RN_2024 || row.rn_2024 || 0),
        rn_2023: parseInt(row.RN_2023 || row.rn_2023 || 0),
        rn_2022: parseInt(row.RN_2022 || row.rn_2022 || 0)
      }));
      await syncToSupabase('market_mainmarket', supabaseData, true);
    }
    console.log('>>> Sync: market_mainmarket');
    return data;
  } catch (err) {
    console.error('>>> Sync hatası (market_mainmarket):', err.message);
    return [];
  } finally {
    if (connection) await connection.close();
  }
}

// Sync: Tüm verileri senkronize et
async function syncAllData() {
  if (!supabase) {
    console.log('>>> Sync atlandı: Supabase yapılandırılmamış');
    return;
  }
  if (!isWithinSyncHours()) {
    console.log('>>> Sync atlandı: Saat 09:00-17:00 dışında');
    return;
  }
  console.log('>>> Periyodik sync başlıyor...');
  await syncTodayMetrics();
  await syncMonthlyData();
  await syncRnHeatmap();
  await syncAlosAdbHeatmap();
  await syncBobRevenueAnalysis();
  await syncTodayAgentRn();
  await syncTodayRnByMonth();
  await syncTodayRnByMonthMarket();
  await syncDailyMarketRn();
  await syncBookingPace();
  await syncAnnualTarget();
  await syncAgentPerformance();
  await syncMarketMainmarket();
  console.log('>>> Periyodik sync tamamlandı');
}

// Periyodik sync başlat
function startPeriodicSync() {
  // İlk sync'i hemen çalıştır (eğer saat uygunsa)
  syncAllData();
  // Sonra her 30 dakikada bir
  setInterval(() => syncAllData(), SYNC_INTERVAL_MS);
  console.log('>>> Periyodik sync aktif: 30 dakikada bir (09:00-17:00)');
}

// Dashboard sayfası (login sonrası gösterilecek) — statik dosyalar public/ içinden
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Mevcut endpoint
app.get('/api/data', async (req, res) => {
  let connection;

  try {
    console.log('>>> Bağlantı kuruluyor...');
    connection = await oracledb.getConnection(dbConfig);
    console.log('>>> BAŞARILI! User:', connection.user);

    const result = await connection.execute(
      `SELECT
    -- 1. MONTH
    CASE t_m.month_num
        WHEN '01' THEN 'January' WHEN '02' THEN 'February' WHEN '03' THEN 'March'
        WHEN '04' THEN 'April' WHEN '05' THEN 'May' WHEN '06' THEN 'June'
        WHEN '07' THEN 'July' WHEN '08' THEN 'August' WHEN '09' THEN 'September'
        WHEN '10' THEN 'October' WHEN '11' THEN 'November' WHEN '12' THEN 'December'
        WHEN '99' THEN 'GRAND TOTAL'
    END AS MONTH,

    -- 2. MARKET
    COALESCE(t_p.market, 'TOTAL') AS MARKET,

    -- 3. 2026 BOB REVENUE
    ROUND(NVL(t_p.bob_2026_revenue, 0), 0) AS "2026 BOB REVENUE",

    -- 4. 2025 BOB REVENUE
    ROUND(NVL(t_p.bob_2025_revenue, 0), 0) AS "2025 BOB REVENUE",

    -- 5. BOB REVENUE DIFF
    ROUND(NVL(t_p.bob_2026_revenue, 0) - NVL(t_p.bob_2025_revenue, 0), 0) AS "BOB REVENUE DIFF",

    -- 6. BOB REVENUE DIFF (%)
    ROUND(
        ( (NVL(t_p.bob_2026_revenue, 0) - NVL(t_p.bob_2025_revenue, 0))
        / NULLIF(NVL(t_p.bob_2025_revenue, 0), 0) )
        * 100
    , 2) AS "BOB REVENUE DIFF (%)",

    -- 7. 2026 BOB ADB
    ROUND(NVL(t_p.bob_2026_revenue, 0) / NULLIF(NVL(t_p.bob_2026_pax, 0), 0), 2) AS "2026 BOB ADB",

    -- 8. 2025 BOB ADB
    ROUND(NVL(t_p.bob_2025_revenue, 0) / NULLIF(NVL(t_p.bob_2025_pax, 0), 0), 2) AS "2025 BOB ADB",

    -- 9. ADB DIFF
    ROUND(
        (NVL(t_p.bob_2026_revenue, 0) / NULLIF(NVL(t_p.bob_2026_pax, 0), 0)) -
        (NVL(t_p.bob_2025_revenue, 0) / NULLIF(NVL(t_p.bob_2025_pax, 0), 0))
    , 2) AS "ADB DIFF",

    -- 10. 2026 BOB RN
    NVL(t_p.bob_2026_rn, 0) AS "2026 BOB RN",

    -- 11. 2025 BOB RN
    NVL(t_p.bob_2025_rn, 0) AS "2025 BOB RN",

    -- 12. RN DIFF
    NVL(t_p.bob_2026_rn, 0) - NVL(t_p.bob_2025_rn, 0) AS "RN DIFF",

    -- 13. RN DIFF (%)
    ROUND(
        ( (NVL(t_p.bob_2026_rn, 0) - NVL(t_p.bob_2025_rn, 0))
        / NULLIF(NVL(t_p.bob_2025_rn, 0), 0) )
        * 100
    , 2) AS "RN DIFF (%)"

FROM (
    SELECT '01' month_num FROM dual UNION ALL SELECT '02' FROM dual UNION ALL SELECT '03' FROM dual UNION ALL
    SELECT '04' FROM dual UNION ALL SELECT '05' FROM dual UNION ALL SELECT '06' FROM dual UNION ALL
    SELECT '07' FROM dual UNION ALL SELECT '08' FROM dual UNION ALL SELECT '09' FROM dual UNION ALL
    SELECT '10' FROM dual UNION ALL SELECT '11' FROM dual UNION ALL SELECT '12' FROM dual UNION ALL
    SELECT '99' FROM dual  -- GRAND TOTAL için özel kod
) t_m

LEFT JOIN (
    -- Pazar bazında detay
    SELECT
        TO_CHAR(r.detail_date, 'MM') AS month_num,
        r.mainmarketcode_long AS market,

        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2026' THEN
              CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                   ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END
             END) AS bob_2026_revenue,
        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2026' THEN r.noofadults + NVL(r.ca3, 0) / 2 END) AS bob_2026_pax,
        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2026' THEN 1 END) AS bob_2026_rn,

        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2025' THEN
              CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                   ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END
             END) AS bob_2025_revenue,
        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2025' THEN r.noofadults + NVL(r.ca3, 0) / 2 END) AS bob_2025_pax,
        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2025' THEN 1 END) AS bob_2025_rn

    FROM v8live.pro_isv_reservationinfo_voyage r
    LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
    LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013

    WHERE r.reservationstatus = 1
      AND TO_CHAR(r.detail_date, 'YYYY') IN ('2025', '2026')
      AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -12 * (2026 - TO_NUMBER(TO_CHAR(r.detail_date, 'YYYY'))))

    GROUP BY TO_CHAR(r.detail_date, 'MM'), r.mainmarketcode_long

    UNION ALL

    -- Aylık TOTAL satırı
    SELECT
        TO_CHAR(r.detail_date, 'MM') AS month_num,
        NULL AS market,

        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2026' THEN
              CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                   ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END
             END) AS bob_2026_revenue,
        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2026' THEN r.noofadults + NVL(r.ca3, 0) / 2 END) AS bob_2026_pax,
        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2026' THEN 1 END) AS bob_2026_rn,

        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2025' THEN
              CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                   ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END
             END) AS bob_2025_revenue,
        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2025' THEN r.noofadults + NVL(r.ca3, 0) / 2 END) AS bob_2025_pax,
        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2025' THEN 1 END) AS bob_2025_rn

    FROM v8live.pro_isv_reservationinfo_voyage r
    LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
    LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013

    WHERE r.reservationstatus = 1
      AND TO_CHAR(r.detail_date, 'YYYY') IN ('2025', '2026')
      AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -12 * (2026 - TO_NUMBER(TO_CHAR(r.detail_date, 'YYYY'))))

    GROUP BY TO_CHAR(r.detail_date, 'MM')

    UNION ALL

    -- GRAND TOTAL pazar bazında
    SELECT
        '99' AS month_num,  -- GRAND TOTAL için özel kod
        r.mainmarketcode_long AS market,

        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2026' THEN
              CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                   ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END
             END) AS bob_2026_revenue,
        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2026' THEN r.noofadults + NVL(r.ca3, 0) / 2 END) AS bob_2026_pax,
        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2026' THEN 1 END) AS bob_2026_rn,

        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2025' THEN
              CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                   ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END
             END) AS bob_2025_revenue,
        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2025' THEN r.noofadults + NVL(r.ca3, 0) / 2 END) AS bob_2025_pax,
        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2025' THEN 1 END) AS bob_2025_rn

    FROM v8live.pro_isv_reservationinfo_voyage r
    LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
    LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013

    WHERE r.reservationstatus = 1
      AND TO_CHAR(r.detail_date, 'YYYY') IN ('2025', '2026')
      AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -12 * (2026 - TO_NUMBER(TO_CHAR(r.detail_date, 'YYYY'))))

    GROUP BY r.mainmarketcode_long

    UNION ALL

    -- GRAND TOTAL genel (tüm pazarlar toplamı)
    SELECT
        '99' AS month_num,
        NULL AS market,

        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2026' THEN
              CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                   ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END
             END) AS bob_2026_revenue,
        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2026' THEN r.noofadults + NVL(r.ca3, 0) / 2 END) AS bob_2026_pax,
        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2026' THEN 1 END) AS bob_2026_rn,

        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2025' THEN
              CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                   ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END
             END) AS bob_2025_revenue,
        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2025' THEN r.noofadults + NVL(r.ca3, 0) / 2 END) AS bob_2025_pax,
        SUM(CASE WHEN TO_CHAR(r.detail_date, 'YYYY') = '2025' THEN 1 END) AS bob_2025_rn

    FROM v8live.pro_isv_reservationinfo_voyage r
    LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
    LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013

    WHERE r.reservationstatus = 1
      AND TO_CHAR(r.detail_date, 'YYYY') IN ('2025', '2026')
      AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -12 * (2026 - TO_NUMBER(TO_CHAR(r.detail_date, 'YYYY'))))

) t_p ON t_m.month_num = t_p.month_num

ORDER BY t_m.month_num, CASE WHEN t_p.market IS NULL THEN 'ZZZZZ' ELSE t_p.market END`,
      [],
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    console.log('>>> Tablo sayısı:', result.rows.length);
    res.json(result.rows);

  } catch (err) {
    console.error('>>> HATA:', err.message);
    res.status(500).json({ error: err.message });
  } finally {
    if (connection) {
      await connection.close();
    }
  }
});

// YENİ ENDPOINT: Bugünün metrikleri
app.get('/api/today-metrics', async (req, res) => {
  let connection;

  try {
    console.log('>>> Bugünün metrikleri çekiliyor...');
    connection = await oracledb.getConnection(dbConfig);

    const result = await connection.execute(
      `SELECT
    COUNT(DISTINCT r.reservation_id) AS today_reservations,
    COUNT(*) AS today_rn,
    ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local'
                   THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                   ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0)
              END), 0) AS today_revenue
FROM v8live.pro_isv_reservationinfo_voyage r
LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
WHERE r.reservationstatus = 1
  AND TRUNC(r.saledate) = TRUNC(SYSDATE)
  AND TO_CHAR(r.detail_date, 'YYYY') = '2026'`,
      [],
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    console.log('>>> Bugünün metrikleri başarılı');
    const data = result.rows[0] || { today_reservations: 0, today_rn: 0, today_revenue: 0 };
    
    // Supabase'e yaz
    if (supabase) {
      await upsertToSupabase('today_metrics', {
        today_reservations: parseInt(data.TODAY_RESERVATIONS || data.today_reservations || 0),
        today_rn: parseInt(data.TODAY_RN || data.today_rn || 0),
        today_revenue: parseFloat(data.TODAY_REVENUE || data.today_revenue || 0)
      }, false);
    }
    
    res.json(data);

  } catch (err) {
    console.error('>>> HATA (today-metrics):', err.message);
    res.status(500).json({ error: err.message });
  } finally {
    if (connection) {
      await connection.close();
    }
  }
});

// Bugün girilen rezervasyonlar: acente (AGENTNAME) bazlı RN ve toplam gelir
app.get('/api/today-agent-rn', async (req, res) => {
  try {
    const data = await syncTodayAgentRn();
    res.json(Array.isArray(data) ? data : []);
  } catch (err) {
    console.error('>>> HATA (today-agent-rn):', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Bugün girilen rezervasyonların 2026 aylarına göre room night dağılımı
app.get('/api/today-rn-by-month', async (req, res) => {
  try {
    const data = await syncTodayRnByMonth();
    res.json(Array.isArray(data) ? data : []);
  } catch (err) {
    console.error('>>> HATA (today-rn-by-month):', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Bugün girilen rezervasyonlar: ay + pazar (Günlük Doluluk ile aynı pazar sırası/renk)
app.get('/api/today-rn-by-month-market', async (req, res) => {
  try {
    const data = await syncTodayRnByMonthMarket();
    res.json(Array.isArray(data) ? data : []);
  } catch (err) {
    console.error('>>> HATA (today-rn-by-month-market):', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Günlük pazar bazlı RN: 2026 tüm yıl OTB toplamına göre en büyük pazarlar (limit 15)
app.get('/api/daily-market-rn', async (req, res) => {
  try {
    const responseData = await syncDailyMarketRn();
    const empty = { data2026: [], daily_2025_totals: [], daily_2024_totals: [], daily_2023_totals: [], daily_2022_totals: [] };
    res.json(responseData || empty);
  } catch (err) {
    console.error('>>> HATA (daily-market-rn):', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Booking Pace: Son 30/15 gün + 2025 pace
app.get('/api/booking-pace', async (req, res) => {
  try {
    const data = await syncBookingPace();
    res.json(Array.isArray(data) ? data : []);
  } catch (err) {
    console.error('>>> HATA (booking-pace):', err.message);
    res.status(500).json({ error: err.message });
  }
});

// 2026 toplam OTB geliri (Yıllık Gelir Hedefi gauge için)
app.get('/api/annual-target', async (req, res) => {
  try {
    const data = await syncAnnualTarget();
    res.json(data && data.TOTAL_REVENUE_2026 !== undefined ? data : { TOTAL_REVENUE_2026: 0 });
  } catch (err) {
    console.error('>>> HATA (annual-target):', err.message);
    res.status(500).json({ error: err.message });
  }
});

// 2026 + 2025 OTB aylık veriler (RN, revenue, avg_rate; 2025 = aynı gün geçen yıl OTB)
app.get('/api/monthly-data', async (req, res) => {
  let connection;

  try {
    console.log('>>> Aylık veriler çekiliyor...');
    connection = await oracledb.getConnection(dbConfig);

    const result = await connection.execute(
      `WITH m AS (
          SELECT '01' month_num FROM dual UNION ALL SELECT '02' FROM dual UNION ALL SELECT '03' FROM dual UNION ALL
          SELECT '04' FROM dual UNION ALL SELECT '05' FROM dual UNION ALL SELECT '06' FROM dual UNION ALL
          SELECT '07' FROM dual UNION ALL SELECT '08' FROM dual UNION ALL SELECT '09' FROM dual UNION ALL
          SELECT '10' FROM dual UNION ALL SELECT '11' FROM dual UNION ALL SELECT '12' FROM dual
      ),
      d2026 AS (
          SELECT TO_CHAR(r.detail_date, 'MM') AS month_num,
                 COUNT(*) AS total_rn,
                 ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                               ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END), 0) AS total_revenue,
                 ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                               ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END)
                       / NULLIF(SUM(r.noofadults + NVL(r.ca3, 0) / 2), 0), 2) AS avg_rate
          FROM v8live.pro_isv_reservationinfo_voyage r
          LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
          LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
          WHERE r.reservationstatus = 1 AND TO_CHAR(r.detail_date, 'YYYY') = '2026'
            AND r.saledate <= TRUNC(SYSDATE)
          GROUP BY TO_CHAR(r.detail_date, 'MM')
      ),
      d2025 AS (
          SELECT TO_CHAR(r.detail_date, 'MM') AS month_num,
                 COUNT(*) AS total_rn_2025,
                 ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                               ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END), 0) AS total_revenue_2025,
                 ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                               ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END)
                       / NULLIF(SUM(r.noofadults + NVL(r.ca3, 0) / 2), 0), 2) AS avg_rate_2025
          FROM v8live.pro_isv_reservationinfo_voyage r
          LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
          LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
          WHERE r.reservationstatus = 1 AND TO_CHAR(r.detail_date, 'YYYY') = '2025'
            AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -12)
          GROUP BY TO_CHAR(r.detail_date, 'MM')
      ),
      d2024 AS (
          SELECT TO_CHAR(r.detail_date, 'MM') AS month_num,
                 COUNT(*) AS total_rn_2024,
                 ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                               ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END), 0) AS total_revenue_2024,
                 ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                               ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END)
                       / NULLIF(SUM(r.noofadults + NVL(r.ca3, 0) / 2), 0), 2) AS avg_rate_2024
          FROM v8live.pro_isv_reservationinfo_voyage r
          LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
          LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
          WHERE r.reservationstatus = 1 AND TO_CHAR(r.detail_date, 'YYYY') = '2024'
            AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -24)
          GROUP BY TO_CHAR(r.detail_date, 'MM')
      ),
      d2023 AS (
          SELECT TO_CHAR(r.detail_date, 'MM') AS month_num,
                 COUNT(*) AS total_rn_2023,
                 ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                               ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END), 0) AS total_revenue_2023,
                 ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                               ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END)
                       / NULLIF(SUM(r.noofadults + NVL(r.ca3, 0) / 2), 0), 2) AS avg_rate_2023
          FROM v8live.pro_isv_reservationinfo_voyage r
          LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
          LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
          WHERE r.reservationstatus = 1 AND TO_CHAR(r.detail_date, 'YYYY') = '2023'
            AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -36)
          GROUP BY TO_CHAR(r.detail_date, 'MM')
      ),
      d2022 AS (
          SELECT TO_CHAR(r.detail_date, 'MM') AS month_num,
                 COUNT(*) AS total_rn_2022,
                 ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                               ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END), 0) AS total_revenue_2022,
                 ROUND(SUM(CASE WHEN r.mainmarketcode_long = 'Local' THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                               ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END)
                       / NULLIF(SUM(r.noofadults + NVL(r.ca3, 0) / 2), 0), 2) AS avg_rate_2022
          FROM v8live.pro_isv_reservationinfo_voyage r
          LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
          LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
          WHERE r.reservationstatus = 1 AND TO_CHAR(r.detail_date, 'YYYY') = '2022'
            AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -48)
          GROUP BY TO_CHAR(r.detail_date, 'MM')
      )
      SELECT
          CASE m.month_num WHEN '01' THEN 'Jan' WHEN '02' THEN 'Feb' WHEN '03' THEN 'Mar' WHEN '04' THEN 'Apr'
              WHEN '05' THEN 'May' WHEN '06' THEN 'Jun' WHEN '07' THEN 'Jul' WHEN '08' THEN 'Aug'
              WHEN '09' THEN 'Sep' WHEN '10' THEN 'Oct' WHEN '11' THEN 'Nov' WHEN '12' THEN 'Dec' END AS month,
          m.month_num,
          NVL(d2026.total_rn, 0) AS total_rn,
          NVL(d2026.total_revenue, 0) AS total_revenue,
          NVL(d2026.avg_rate, 0) AS avg_rate,
          NVL(d2025.total_rn_2025, 0) AS total_rn_2025,
          NVL(d2025.total_revenue_2025, 0) AS total_revenue_2025,
          NVL(d2025.avg_rate_2025, 0) AS avg_rate_2025,
          NVL(d2024.total_rn_2024, 0) AS total_rn_2024,
          NVL(d2024.total_revenue_2024, 0) AS total_revenue_2024,
          NVL(d2024.avg_rate_2024, 0) AS avg_rate_2024,
          NVL(d2023.total_rn_2023, 0) AS total_rn_2023,
          NVL(d2023.total_revenue_2023, 0) AS total_revenue_2023,
          NVL(d2023.avg_rate_2023, 0) AS avg_rate_2023,
          NVL(d2022.total_rn_2022, 0) AS total_rn_2022,
          NVL(d2022.total_revenue_2022, 0) AS total_revenue_2022,
          NVL(d2022.avg_rate_2022, 0) AS avg_rate_2022
      FROM m
      LEFT JOIN d2026 ON m.month_num = d2026.month_num
      LEFT JOIN d2025 ON m.month_num = d2025.month_num
      LEFT JOIN d2024 ON m.month_num = d2024.month_num
      LEFT JOIN d2023 ON m.month_num = d2023.month_num
      LEFT JOIN d2022 ON m.month_num = d2022.month_num
      ORDER BY m.month_num`,
      [],
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    console.log('>>> Aylık veriler başarılı:', result.rows.length, 'ay');
    const data = result.rows || [];
    
    // Supabase'e yaz
    if (supabase && data.length > 0) {
      const supabaseData = data.map(row => ({
        month_num: row.MONTH_NUM || row.month_num || '',
        month_label: row.MONTH || row.month || '',
        total_rn: parseInt(row.TOTAL_RN || row.total_rn || 0),
        total_revenue: parseFloat(row.TOTAL_REVENUE || row.total_revenue || 0),
        avg_rate: parseFloat(row.AVG_RATE || row.avg_rate || 0),
        total_rn_2025: parseInt(row.TOTAL_RN_2025 || row.total_rn_2025 || 0),
        total_revenue_2025: parseFloat(row.TOTAL_REVENUE_2025 || row.total_revenue_2025 || 0),
        avg_rate_2025: parseFloat(row.AVG_RATE_2025 || row.avg_rate_2025 || 0),
        total_rn_2024: parseInt(row.TOTAL_RN_2024 || row.total_rn_2024 || 0),
        total_revenue_2024: parseFloat(row.TOTAL_REVENUE_2024 || row.total_revenue_2024 || 0),
        avg_rate_2024: parseFloat(row.AVG_RATE_2024 || row.avg_rate_2024 || 0),
        total_rn_2023: parseInt(row.TOTAL_RN_2023 || row.total_rn_2023 || 0),
        total_revenue_2023: parseFloat(row.TOTAL_REVENUE_2023 || row.total_revenue_2023 || 0),
        avg_rate_2023: parseFloat(row.AVG_RATE_2023 || row.avg_rate_2023 || 0),
        total_rn_2022: parseInt(row.TOTAL_RN_2022 || row.total_rn_2022 || 0),
        total_revenue_2022: parseFloat(row.TOTAL_REVENUE_2022 || row.total_revenue_2022 || 0),
        avg_rate_2022: parseFloat(row.AVG_RATE_2022 || row.avg_rate_2022 || 0)
      }));
      await syncToSupabase('monthly_data', supabaseData, false);
    }
    
    res.json(data);

  } catch (err) {
    console.error('>>> HATA (monthly-data):', err.message);
    res.status(500).json({ error: err.message });
  } finally {
    if (connection) {
      await connection.close();
    }
  }
});

// RN Heatmap verisi: PIVOT sorgusu (detail_date >= TRUNC(SYSDATE), 2025–2026), RN/ADB string → parse edilmiş satırlar
app.get('/api/rn-heatmap', async (req, res) => {
  let connection;

  function parseRnAdb(val) {
    if (val == null || typeof val !== 'string') return null;
    const parts = val.trim().split(/\s*\/\s*/);
    if (parts.length < 2) return null;
    const rn = parseFloat(parts[0].replace(/,/g, '').trim());
    const price = parseFloat(parts[1].replace(/,/g, '').trim());
    if (isNaN(rn) || isNaN(price)) return null;
    return { rn, price };
  }

  try {
    console.log('>>> RN heatmap verileri çekiliyor (PIVOT sorgusu)...');
    connection = await oracledb.getConnection(dbConfig);

    const result = await connection.execute(
      `SELECT * FROM (
          SELECT
              NVL(Stay_Ay, 'TOPLAM') AS Stay_Ay,
              NVL(Pazar, 'GENEL TOPLAM') AS Pazar,
              Oda_Tipi,
              TO_CHAR(SUM(RN_ODA), 'FM999,990') || ' / ' ||
              TO_CHAR(ROUND(SUM(Rev) / NULLIF(SUM(PAX_HESABI), 0), 2), 'FM999,990.00') AS RN_ADB
          FROM (
              SELECT
                  TO_CHAR(r.detail_date, 'YYYY-MM') AS Stay_Ay,
                  r.mainmarketcode_long AS Pazar,
                  r.rateroomtype AS Oda_Tipi,
                  1 AS RN_ODA,
                  (r.noofadults + NVL(r.ca3, 0) / 2) AS PAX_HESABI,
                  CASE WHEN r.mainmarketcode_long = 'Local'
                       THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                       ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0)
                  END AS Rev
              FROM v8live.pro_isv_reservationinfo_voyage r
              LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
              LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
              WHERE r.reservationstatus = 1
                AND r.detail_date >= TRUNC(SYSDATE)
                AND TO_CHAR(r.detail_date, 'YYYY') IN ('2025', '2026')

              UNION ALL

              SELECT
                  TO_CHAR(r.detail_date, 'YYYY-MM') AS Stay_Ay,
                  r.mainmarketcode_long AS Pazar,
                  'TUM_ODALAR' AS Oda_Tipi,
                  1 AS RN_ODA,
                  (r.noofadults + NVL(r.ca3, 0) / 2) AS PAX_HESABI,
                  CASE WHEN r.mainmarketcode_long = 'Local'
                       THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                       ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0)
                  END AS Rev
              FROM v8live.pro_isv_reservationinfo_voyage r
              LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
              LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
              WHERE r.reservationstatus = 1
                AND r.detail_date >= TRUNC(SYSDATE)
                AND TO_CHAR(r.detail_date, 'YYYY') IN ('2025', '2026')
          )
          GROUP BY ROLLUP(Stay_Ay, Pazar), Oda_Tipi
      )
      PIVOT (
          MAX(RN_ADB)
          FOR Oda_Tipi IN (
              'VILLA' AS "BUNGALOV_236",
              'OTEL'  AS "STANDART_LAND_VIEW_57",
              'ODNZ'  AS "STANDART_SEA_VIEW_70",
              'FAM'   AS "BUNGALOV_AILE_133",
              'OFAM'  AS "STANDART_FAMILY_8",
              'SUIT'  AS "SUITE_4",
              'TUM_ODALAR' AS "TOPLAM_508"
          )
      )
      ORDER BY
          CASE WHEN Stay_Ay = 'TOPLAM' THEN 2 ELSE 1 END,
          Stay_Ay,
          CASE WHEN Pazar = 'GENEL TOPLAM' THEN 2 ELSE 1 END,
          Pazar`,
      [],
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    const rows = result.rows || [];
    const colMap = [
      { key: 'BUNGALOV_236', id: 'BUNGALOV' },
      { key: 'STANDART_LAND_VIEW_57', id: 'STANDART_LAND_VIEW' },
      { key: 'STANDART_SEA_VIEW_70', id: 'STANDART_SEA_VIEW' },
      { key: 'BUNGALOV_AILE_133', id: 'BUNGALOV_AILE_ODASI' },
      { key: 'STANDART_FAMILY_8', id: 'STANDART_FAMILY_ROOM' },
      { key: 'SUITE_4', id: 'SUITE' },
      { key: 'TOPLAM_508', id: 'TUM_ODALAR' }
    ];

    const out = [];
    let yearTotalRn = null;
    const lastColKey = 'TOPLAM_508';

    rows.forEach((row) => {
      const stayAy = row.STAY_AY || row.Stay_Ay || row.stay_ay;
      const pazar = row.PAZAR || row.Pazar || row.pazar;
      if (!stayAy || !pazar) return;

      // En alt satır: Stay_Ay='TOPLAM', Pazar='GENEL TOPLAM' → yıllık toplam RN (en son sütun)
      if (stayAy === 'TOPLAM' && pazar === 'GENEL TOPLAM') {
        const val = row[lastColKey] != null ? row[lastColKey] : (row[lastColKey.toUpperCase()]);
        const parsed = parseRnAdb(val);
        if (parsed && !isNaN(parsed.rn)) yearTotalRn = Math.round(parsed.rn);
        return;
      }

      colMap.forEach(({ key, id }) => {
        const val = row[key] != null ? row[key] : (row[key.toUpperCase()]);
        const parsed = parseRnAdb(val);
        if (parsed && (parsed.rn > 0 || parsed.price > 0)) {
          out.push({
            month_key: stayAy,
            market: pazar,
            room_type: id,
            rn: parsed.rn,
            price: parsed.price
          });
        }
      });
    });

    console.log('>>> RN heatmap başarılı:', rows.length, 'pivot satır ->', out.length, 'kayıt', yearTotalRn != null ? ', yearTotalRn=' + yearTotalRn : '');
    res.json({ rows: out, yearTotalRn: yearTotalRn });
  } catch (err) {
    console.error('>>> HATA (rn-heatmap):', err.message);
    res.status(500).json({ error: err.message });
  } finally {
    if (connection) {
      await connection.close();
    }
  }
});

// Mainmarket bazında 2023–2026 BOB Revenue ve RN kırılımı (pazar payı + gelir grafikleri için)
app.get('/api/market-mainmarket', async (req, res) => {
  try {
    const data = await syncMarketMainmarket();
    res.json(Array.isArray(data) ? data : []);
  } catch (err) {
    console.error('>>> HATA (market-mainmarket):', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Acente Performansı: Top 20 acente, 2026 BOB Revenue mainmarket'e göre stacked (pacing: saledate bugüne kadar)
app.get('/api/agent-performance', async (req, res) => {
  try {
    const data = await syncAgentPerformance();
    res.json(Array.isArray(data) ? data : []);
  } catch (err) {
    console.error('>>> HATA (agent-performance):', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ALOS & ADB Heatmap: Pivot 24 ay (2025 ve 2026 tüm aylar), tek ALOS_ADB per ay; 2025 saledate<=geçen yıl bugün, 2026 saledate<=bugün
app.get('/api/alos-adb-heatmap', async (req, res) => {
  let connection;
  try {
    console.log('>>> ALOS & ADB heatmap verileri çekiliyor...');
    connection = await oracledb.getConnection(dbConfig);
    const result = await connection.execute(
      `SELECT * FROM (
    SELECT
        TO_CHAR(r.detail_date, 'YYYY-MM') AS Ay,
        r.mainmarketcode_long AS Pazar,
        ROUND(AVG(r.departuredate - r.arrivaldate), 1) || ' ; ' ||
        ROUND(
            SUM(CASE WHEN r.mainmarketcode_long = 'Local'
                     THEN r.netpostamount / NULLIF(ex_s.exch_rate_day, 0)
                     ELSE r.netpostamount / NULLIF(ex_d.exch_rate_day, 0) END)
            / NULLIF(SUM(r.departuredate - r.arrivaldate), 0), 2
        ) AS ALOS_ADB
    FROM
        v8live.pro_isv_reservationinfo_voyage r
        LEFT JOIN pro_isv_exchangerates ex_s ON ex_s.wdat_date = r.saledate AND ex_s.zcur_id = 1013
        LEFT JOIN pro_isv_exchangerates ex_d ON ex_d.wdat_date = r.detail_date AND ex_d.zcur_id = 1013
    WHERE
        r.reservationstatus = 1
        AND (r.departuredate - r.arrivaldate) > 0
        AND (
            (TO_CHAR(r.detail_date, 'YYYY') = '2025' AND r.saledate <= ADD_MONTHS(TRUNC(SYSDATE), -12))
            OR
            (TO_CHAR(r.detail_date, 'YYYY') = '2026' AND r.saledate <= TRUNC(SYSDATE))
        )
    GROUP BY
        TO_CHAR(r.detail_date, 'YYYY-MM'),
        r.mainmarketcode_long
)
PIVOT (
    MAX(ALOS_ADB)
    FOR Ay IN (
        '2025-01' AS JAN_25, '2025-02' AS FEB_25, '2025-03' AS MAR_25, '2025-04' AS APR_25,
        '2025-05' AS MAY_25, '2025-06' AS JUN_25, '2025-07' AS JUL_25, '2025-08' AS AUG_25,
        '2025-09' AS SEP_25, '2025-10' AS OCT_25, '2025-11' AS NOV_25, '2025-12' AS DEC_25,
        '2026-01' AS JAN_26, '2026-02' AS FEB_26, '2026-03' AS MAR_26, '2026-04' AS APR_26,
        '2026-05' AS MAY_26, '2026-06' AS JUN_26, '2026-07' AS JUL_26, '2026-08' AS AUG_26,
        '2026-09' AS SEP_26, '2026-10' AS OCT_26, '2026-11' AS NOV_26, '2026-12' AS DEC_26
    )
)
ORDER BY Pazar`,
      [],
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );
    console.log('>>> ALOS & ADB heatmap başarılı:', result.rows.length, 'satır');
    res.json(result.rows);
  } catch (err) {
    console.error('>>> HATA (alos-adb-heatmap):', err.message);
    res.status(500).json({ error: err.message });
  } finally {
    if (connection) {
      await connection.close();
    }
  }
});

// BOB Revenue Analysis sync (manuel tetikleme)
app.get('/api/sync-bob-revenue', async (req, res) => {
  try {
    await syncBobRevenueAnalysis();
    return res.json({ success: true, message: 'bob_revenue_analysis sync tamamlandı' });
  } catch (err) {
    console.error('>>> /api/sync-bob-revenue:', err.message);
    return res.status(500).json({ success: false, error: err.message });
  }
});

// Supabase bağlantı durumu (test için)
app.get('/api/supabase-status', async (req, res) => {
  if (!supabase) {
    return res.json({
      connected: false,
      message: 'Supabase yapılandırılmamış. .env içinde SUPABASE_URL ve SUPABASE_SERVICE_ROLE_KEY tanımlayın.'
    });
  }
  try {
    const { data, error } = await supabase.from('today_metrics').select('id').limit(1);
    return res.json({
      connected: true,
      message: error ? 'Bağlantı var, tablo erişim hatası: ' + error.message : 'Supabase bağlantısı OK',
      tableCheck: error ? null : (data || [])
    });
  } catch (err) {
    return res.status(500).json({
      connected: false,
      message: 'Supabase hatası: ' + err.message
    });
  }
});

app.listen(PORT, () => {
  console.log('=== Server hazır: http://localhost:' + PORT + ' ===');
  if (!supabase) {
    console.log('>>> Supabase: .env içinde SUPABASE_URL ve SUPABASE_SERVICE_ROLE_KEY tanımlayın.');
  } else {
    console.log('>>> Supabase: Bağlantı yapılandırıldı.');
    startPeriodicSync();
  }
});