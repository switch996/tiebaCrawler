PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS forum_state (
  forum TEXT PRIMARY KEY,
  last_crawl_ts INTEGER,
  updated_at TEXT NOT NULL
);

-- Main threads table
CREATE TABLE IF NOT EXISTS threads (
  tid INTEGER PRIMARY KEY,
  fid INTEGER,
  fname TEXT,

  title TEXT,

  author_id INTEGER,
  author_name TEXT,

  agree INTEGER,              -- 点赞数
  pid INTEGER,                -- 首楼回复pid

  create_time INTEGER,        -- 创建时间（秒级时间戳）
  last_time INTEGER,          -- 最后回复时间（秒级时间戳）

  reply_num INTEGER,
  view_num INTEGER,

  is_top INTEGER,
  is_good INTEGER,
  is_help INTEGER,
  is_hide INTEGER,
  is_share INTEGER,

  text TEXT,                  -- th.contents.text
  contents_json TEXT,         -- structured content

  -- labeling / routing
  category TEXT,              -- AI label, e.g. "交友贴"
  tags_json TEXT,             -- optional JSON array of tags
  thread_role TEXT NOT NULL DEFAULT 'normal', -- 'normal' | 'collection'
  collection_year INTEGER,    -- for collection threads, parsed from title
  collection_week INTEGER,    -- for collection threads, parsed from title

  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_threads_create_time ON threads(create_time);
CREATE INDEX IF NOT EXISTS idx_threads_category ON threads(category);
CREATE INDEX IF NOT EXISTS idx_threads_role ON threads(thread_role);

-- Images download queue
CREATE TABLE IF NOT EXISTS images (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tid INTEGER NOT NULL,
  url TEXT NOT NULL,
  hash TEXT,
  origin_src TEXT,
  src TEXT,
  big_src TEXT,
  show_width INTEGER,
  show_height INTEGER,
  updated_at TEXT NOT NULL,
  UNIQUE(tid, url),
  FOREIGN KEY (tid) REFERENCES threads(tid) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_images_status ON images(status);

-- Relay queue: reply labeled threads into weekly collection threads
CREATE TABLE IF NOT EXISTS relay_tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_tid INTEGER NOT NULL,
  target_tid INTEGER NOT NULL,
  target_forum TEXT NOT NULL,

  category TEXT,          -- label that triggered this relay
  source_year INTEGER,    -- computed from source create_time in timezone
  source_week INTEGER,

  status TEXT NOT NULL,   -- PENDING | POSTING | DONE | ERROR | SKIPPED
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,

  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,

  UNIQUE(source_tid, target_tid),
  FOREIGN KEY (source_tid) REFERENCES threads(tid) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_relay_tasks_status ON relay_tasks(status);
