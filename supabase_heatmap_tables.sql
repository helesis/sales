-- Heatmap verileri için Supabase tabloları
-- Supabase Dashboard → SQL Editor'da çalıştırın.

-- 1) RN Heatmap satırları (ay, pazar, oda tipi, rn, price)
CREATE TABLE IF NOT EXISTS rn_heatmap (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    month_key TEXT NOT NULL,
    market TEXT NOT NULL,
    room_type TEXT NOT NULL,
    rn NUMERIC NOT NULL DEFAULT 0,
    price NUMERIC NOT NULL DEFAULT 0
);

-- 2) RN Heatmap meta (yıllık toplam RN)
CREATE TABLE IF NOT EXISTS rn_heatmap_meta (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    key TEXT NOT NULL,
    value NUMERIC
);

-- 3) ALOS & ADB Heatmap (tek satır, tüm pivot verisi JSON)
CREATE TABLE IF NOT EXISTS alos_adb_heatmap (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    data JSONB NOT NULL DEFAULT '[]'::jsonb
);

-- RLS: anon rolü sadece SELECT yapabilsin (sayfalar anon key ile okuyacak)
ALTER TABLE rn_heatmap ENABLE ROW LEVEL SECURITY;
ALTER TABLE rn_heatmap_meta ENABLE ROW LEVEL SECURITY;
ALTER TABLE alos_adb_heatmap ENABLE ROW LEVEL SECURITY;

CREATE POLICY "anon_select_rn_heatmap" ON rn_heatmap FOR SELECT TO anon USING (true);
CREATE POLICY "anon_select_rn_heatmap_meta" ON rn_heatmap_meta FOR SELECT TO anon USING (true);
CREATE POLICY "anon_select_alos_adb_heatmap" ON alos_adb_heatmap FOR SELECT TO anon USING (true);

-- Service role sync için INSERT/DELETE (server SUPABASE_SERVICE_ROLE_KEY kullanıyor, RLS bypass)
-- Bu politikalar olmadan service role zaten tüm işlemlere erişir.

COMMENT ON TABLE rn_heatmap IS 'RN Heatmap satırları (server sync ile doldurulur)';
COMMENT ON TABLE rn_heatmap_meta IS 'RN Heatmap meta (year_total_rn)';
COMMENT ON TABLE alos_adb_heatmap IS 'ALOS/ADB Heatmap pivot verisi (tek satır JSON)';
