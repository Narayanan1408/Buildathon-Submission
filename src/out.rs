//! Parquet output. Strings are written from one shared buffer per column
//! (no per-row allocation); no dictionary encoding and no statistics, which
//! the checker does not need and which cost time to compute.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

use crate::text::{Bytes, Key, is_long, key_bytes};

pub const NO_TOP: u16 = u16::MAX;

/// user_daily rows, column by column.
#[derive(Default)]
pub struct UserRows {
    pub name_buf: Vec<u8>,
    pub name_end: Vec<usize>,
    pub day: Vec<i32>,
    pub sessions: Vec<i64>,
    pub events: Vec<i64>,
    pub revenue: Vec<i64>,
    pub top: Vec<u16>,
}

impl UserRows {
    #[inline]
    pub fn push_name(&mut self, k: Key) {
        if is_long(k) {
            self.name_buf.extend_from_slice(&key_bytes(k));
        } else {
            let b = k.to_be_bytes();
            self.name_buf.extend_from_slice(&b[..b[15] as usize]);
        }
        self.name_end.push(self.name_buf.len());
    }
    pub fn len(&self) -> usize {
        self.day.len()
    }
}

const USER_SCHEMA: &str = "
message user_daily {
  required binary user_id (UTF8);
  required int32 day (DATE);
  required int64 sessions;
  required int64 events;
  required int64 revenue_cents;
  optional binary top_category (UTF8);
}";

const CAMP_SCHEMA: &str = "
message campaign_daily {
  required binary campaign_id (UTF8);
  required binary channel (UTF8);
  required int32 day (DATE);
  required int64 sessions;
  required int64 purchasing_sessions;
  required int64 revenue_cents;
  required int64 assisted_sessions;
}";

enum Col {
    Str(Vec<ByteArray>, Option<Vec<i16>>),
    I32(Vec<i32>),
    I64(Vec<i64>),
}

fn write_parquet(path: &Path, schema: &str, cols: Vec<Col>, nrows: usize) {
    let schema = Arc::new(parse_message_type(schema).expect("schema"));
    let props = Arc::new(
        WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .build(),
    );
    let file = File::create(path).unwrap_or_else(|e| panic!("create {}: {e}", path.display()));
    let mut w = SerializedFileWriter::new(file, schema, props).expect("parquet writer");
    if nrows > 0 {
        let mut rg = w.next_row_group().expect("row group");
        for col in cols {
            let mut cw = rg.next_column().expect("column").expect("column exists");
            match col {
                Col::Str(v, def) => {
                    cw.typed::<ByteArrayType>().write_batch(&v, def.as_deref(), None).expect("write str");
                }
                Col::I32(v) => {
                    cw.typed::<Int32Type>().write_batch(&v, None, None).expect("write i32");
                }
                Col::I64(v) => {
                    cw.typed::<Int64Type>().write_batch(&v, None, None).expect("write i64");
                }
            }
            cw.close().expect("close column");
        }
        rg.close().expect("close row group");
    }
    w.close().expect("close parquet");
}

fn strings(buf: Vec<u8>, ends: &[usize]) -> Vec<ByteArray> {
    let all = bytes::Bytes::from(buf);
    let mut start = 0;
    ends.iter()
        .map(|&e| {
            let s = ByteArray::from(all.slice(start..e));
            start = e;
            s
        })
        .collect()
}

pub fn write_user_daily(path: &Path, rows: UserRows, cat_names: &[Bytes]) {
    let n = rows.len();
    let cats: Vec<ByteArray> = cat_names.iter().map(|c| ByteArray::from(c.to_vec())).collect();
    let mut def = Vec::with_capacity(n);
    let mut tops = Vec::with_capacity(n);
    for &t in &rows.top {
        if t == NO_TOP {
            def.push(0);
        } else {
            def.push(1);
            tops.push(cats[t as usize].clone());
        }
    }
    let cols = vec![
        Col::Str(strings(rows.name_buf, &rows.name_end), None),
        Col::I32(rows.day),
        Col::I64(rows.sessions),
        Col::I64(rows.events),
        Col::I64(rows.revenue),
        Col::Str(tops, Some(def)),
    ];
    write_parquet(path, USER_SCHEMA, cols, n);
}

/// acc[c * nd + d] = [credited sessions, purchasing sessions, revenue, assisted]
pub fn write_campaign_daily(path: &Path, acc: &[[i64; 4]], camp_ids: &[Bytes], channels: &[Bytes], nd: usize, wsd: i64) {
    let (mut id, mut ch, mut day, mut s, mut ps, mut rev, mut asst) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for (c, (&cid, &chan)) in camp_ids.iter().zip(channels).enumerate() {
        for d in 0..nd {
            let a = acc[c * nd + d];
            if a[0] > 0 || a[3] > 0 {
                id.push(ByteArray::from(cid.to_vec()));
                ch.push(ByteArray::from(chan.to_vec()));
                day.push((wsd + d as i64) as i32);
                s.push(a[0]);
                ps.push(a[1]);
                rev.push(a[2]);
                asst.push(a[3]);
            }
        }
    }
    let n = id.len();
    let cols = vec![
        Col::Str(id, None),
        Col::Str(ch, None),
        Col::I32(day),
        Col::I64(s),
        Col::I64(ps),
        Col::I64(rev),
        Col::I64(asst),
    ];
    write_parquet(path, CAMP_SCHEMA, cols, n);
}
