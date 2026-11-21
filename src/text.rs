//! Low-level text handling: ID keys, reading files, the CSV dialect, timestamps.
//!
//! IDs are handled as `Key`s: a string of up to 15 bytes is packed into a u128
//! (bytes first, length in the last byte), so comparing two keys as numbers is
//! exactly the byte-wise string order. A longer string becomes a 120-bit hash
//! marked by 0xFF in the last byte, and its bytes are kept in a side table.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::{self, File};
use std::hash::BuildHasher;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use foldhash::fast::FixedState as RandomState;
use memmap2::Mmap;

pub type Bytes = &'static [u8];
pub type Key = u128;
pub const CHUNK: usize = 4 << 20;

// ------------------------------------------------------------------ keys

pub const LONG: u8 = 0xFF;

pub fn long_table() -> &'static Mutex<HashMap<Key, Bytes>> {
    static T: OnceLock<Mutex<HashMap<Key, Bytes>>> = OnceLock::new();
    T.get_or_init(|| Mutex::new(HashMap::new()))
}

#[inline]
pub fn key(b: &[u8]) -> Key {
    if b.len() <= 15 {
        let mut buf = [0u8; 16];
        buf[..b.len()].copy_from_slice(b);
        buf[15] = b.len() as u8;
        u128::from_be_bytes(buf)
    } else {
        long_key(b)
    }
}

/// Same as `key`, but reads the bytes with one 16-byte load when that load
/// stays before `end` (the end of the buffer the slice lives in).
#[inline(always)]
pub fn key_fast(b: &[u8], end: *const u8) -> Key {
    let n = b.len();
    if n <= 15 && (b.as_ptr() as usize) + 16 <= end as usize {
        let v = unsafe { std::ptr::read_unaligned(b.as_ptr() as *const u128) };
        let v = u128::from_le(v) & ((1u128 << (8 * n)) - 1);
        v.swap_bytes() | n as u128
    } else {
        key(b)
    }
}

#[cold]
pub fn long_key(b: &[u8]) -> Key {
    let h1 = RandomState::with_seed(0x9e3779b97f4a7c15).hash_one(b);
    let h2 = RandomState::with_seed(0xc2b2ae3d27d4eb4f).hash_one(b);
    let k = ((h1 as u128) << 64) | (((h2 >> 8) as u128) << 8) | LONG as u128;
    long_table().lock().unwrap().entry(k).or_insert_with(|| Box::leak(b.to_vec().into_boxed_slice()));
    k
}

#[inline]
pub fn is_long(k: Key) -> bool {
    k as u8 == LONG
}

pub fn key_bytes(k: Key) -> Vec<u8> {
    if is_long(k) {
        long_table().lock().unwrap()[&k].to_vec()
    } else {
        let buf = k.to_be_bytes();
        buf[..buf[15] as usize].to_vec()
    }
}

/// Byte-wise order of the strings behind two keys.
#[inline]
pub fn key_cmp(a: Key, b: Key) -> Ordering {
    if !is_long(a) && !is_long(b) { a.cmp(&b) } else { key_bytes(a).cmp(&key_bytes(b)) }
}

// ---------------------------------------------------------------- input files

pub fn map_file(p: &Path) -> Bytes {
    let f = File::open(p).unwrap_or_else(|e| panic!("open {}: {e}", p.display()));
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    if len == 0 {
        return &[];
    }
    let m = unsafe { Mmap::map(&f) }.unwrap_or_else(|e| panic!("mmap {}: {e}", p.display()));
    let m: &'static Mmap = Box::leak(Box::new(m));
    &m[..]
}

pub fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            let name = e.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".csv") && !name.starts_with('.') && p.is_file() {
                v.push(p);
            }
        }
    }
    v.sort();
    v
}

/// A byte range of a CSV file; it owns the lines that start inside it.
pub struct Piece {
    file: usize,
    off: u64,
    end: u64,
}

/// The pieces of a set of CSV files, `chunk` bytes each. Planning reads
/// nothing (no serial I/O before the workers start): each worker finds its
/// own line boundaries when it reads a piece. Workers read pieces into their
/// own reused buffer with `read`, so the files are never mapped into memory.
pub struct Pieces {
    files: Vec<(File, u64)>,
    pub pieces: Vec<Piece>,
}

impl Pieces {
    pub fn plan(paths: &[PathBuf]) -> Pieces {
        Self::plan_with(paths, CHUNK as u64)
    }

    pub fn plan_with(paths: &[PathBuf], chunk: u64) -> Pieces {
        let mut files = Vec::new();
        let mut pieces = Vec::new();
        for p in paths {
            let f = File::open(p).unwrap_or_else(|e| panic!("open {}: {e}", p.display()));
            let len = f.metadata().map(|m| m.len()).unwrap_or(0);
            let fi = files.len();
            let mut at = 0;
            while at < len {
                let end = (at + chunk).min(len);
                pieces.push(Piece { file: fi, off: at, end });
                at = end;
            }
            files.push((f, len));
        }
        Pieces { files, pieces }
    }

    /// Read piece `i` into `buf` and return the range of `buf` holding its
    /// lines: every line that starts at a position p with off <= p < end,
    /// where a line starts at 0 or after a newline. The file's first line
    /// (the header) is never included. `buf` keeps at least 64 bytes after
    /// the range, so 16-byte loads near its end stay inside the buffer.
    pub fn read(&self, i: usize, buf: &mut Vec<u8>) -> std::ops::Range<usize> {
        use std::os::unix::fs::FileExt;
        let p = &self.pieces[i];
        let (f, len) = &self.files[p.file];
        let len = *len;
        // one byte before the piece tells whether a line starts at `off`
        let from = p.off.saturating_sub(1);
        let mut n = (p.end - from) as usize;
        if buf.len() < n + 64 {
            buf.resize(n.max(CHUNK + 4096) + 64, 0);
        }
        f.read_exact_at(&mut buf[..n], from).expect("read piece");
        // the first line starts after the first newline at or after off - 1
        // (for the first piece: after the header)
        let start = match memchr::memchr(b'\n', &buf[..n]) {
            Some(j) => j + 1,
            None => return 0..0,
        };
        if start as u64 + from >= p.end {
            return 0..0; // no line starts inside this piece
        }
        // the last line ends at the first newline at or after end - 1
        let mut scanned = (p.end - 1 - from) as usize;
        loop {
            if let Some(j) = memchr::memchr(b'\n', &buf[scanned..n]) {
                return start..scanned + j + 1;
            }
            scanned = n;
            let at = from + n as u64;
            if at >= len {
                return start..n; // last line without a newline
            }
            let more = ((len - at) as usize).min(64 << 10);
            if buf.len() < n + more + 64 {
                buf.resize(n + more + 64, 0);
            }
            f.read_exact_at(&mut buf[n..n + more], at).expect("read piece");
            n += more;
        }
    }
}

/// Everything after the header line.
pub fn body(d: Bytes) -> Bytes {
    match memchr::memchr(b'\n', d) {
        Some(i) => &d[i + 1..],
        None => &[],
    }
}

/// Split every file body into pieces of about CHUNK bytes, cut at line ends.
pub fn chunks(files: &[PathBuf]) -> Vec<Bytes> {
    // read, not mapped: one large read per file is faster than a page fault
    // per page when the file is not in the page cache yet
    let bodies: Vec<Bytes> = files
        .iter()
        .map(|p| body(Box::leak(fs::read(p).unwrap_or_else(|e| panic!("read {}: {e}", p.display())).into_boxed_slice())))
        .collect();
    let mut out = Vec::new();
    for b in bodies {
        let mut s = 0;
        while s < b.len() {
            let mut e = (s + CHUNK).min(b.len());
            if e < b.len() {
                e = match memchr::memchr(b'\n', &b[e..]) {
                    Some(i) => e + i + 1,
                    None => b.len(),
                };
            }
            out.push(&b[s..e]);
            s = e;
        }
    }
    out
}

#[inline]
pub fn for_lines(chunk: Bytes, mut f: impl FnMut(Bytes)) {
    let mut s = 0;
    for e in memchr::memchr_iter(b'\n', chunk) {
        f(&chunk[s..e]);
        s = e + 1;
    }
    if s < chunk.len() {
        f(&chunk[s..]);
    }
}

// ------------------------------------------------------------------ CSV dialect

#[inline]
pub fn unquote(f: &[u8]) -> Option<&[u8]> {
    let n = f.len();
    if n >= 2 && f[0] == b'"' && f[n - 1] == b'"' && memchr::memchr(b'"', &f[1..n - 1]).is_none() {
        Some(&f[1..n - 1])
    } else {
        None
    }
}

/// Split one line into exactly N fields. None means malformed.
/// An empty result field means null.
#[inline]
pub fn split<const N: usize>(line: Bytes) -> Option<[Bytes; N]> {
    let mut out: [Bytes; N] = [&[]; N];
    let mut k = 0;
    let mut start = 0;
    let quoted = memchr::memchr(b'"', line).is_some();
    for i in memchr::memchr_iter(b',', line) {
        if k + 1 >= N {
            return None;
        }
        out[k] = &line[start..i];
        k += 1;
        start = i + 1;
    }
    if k + 1 != N {
        return None;
    }
    out[k] = &line[start..];
    if quoted {
        for f in out.iter_mut() {
            if memchr::memchr(b'"', f).is_some() {
                *f = unquote(f)?;
            }
        }
    }
    Some(out)
}

// ------------------------------------------------------------------ timestamps

#[inline]
pub fn d2(b: &[u8], i: usize) -> Option<i64> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a < 10 && c < 10 { Some((a * 10 + c) as i64) } else { None }
}

pub fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

pub fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => if is_leap(y) { 29 } else { 28 },
    }
}

/// Days since 1970-01-01 for a proleptic Gregorian date.
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// F1, F2 or F3 to epoch milliseconds. None means a bad timestamp.
/// An F3 value too big for i64 is also None (it is outside every window anyway).
#[inline]
pub fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if (n == 20 || n == 25) && b[4] == b'-' {
        if b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
            return None;
        }
        let y = d2(b, 0)? * 100 + d2(b, 2)?;
        let mo = d2(b, 5)?;
        let d = d2(b, 8)?;
        let h = d2(b, 11)?;
        let mi = d2(b, 14)?;
        let s = d2(b, 17)?;
        if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || s > 59 {
            return None;
        }
        let mut t = (days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + s) * 1000;
        if n == 20 {
            if b[19] != b'Z' {
                return None;
            }
        } else {
            let sign = match b[19] {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            if b[22] != b':' {
                return None;
            }
            let oh = d2(b, 20)?;
            let om = d2(b, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            t -= sign * (oh * 60 + om) * 60_000;
        }
        return Some(t);
    }
    if n == 0 {
        return None;
    }
    let mut v: i64 = 0;
    for &c in b {
        let x = c.wrapping_sub(b'0');
        if x > 9 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(x as i64)?;
    }
    Some(v)
}

/// Digits only, 1 to 10,000,000.
#[inline]
pub fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let x = c.wrapping_sub(b'0');
        if x > 9 {
            return None;
        }
        v = v * 10 + x as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    if v == 0 { None } else { Some(v as u32) }
}

pub fn manifest_value(s: &str, key: &str) -> i64 {
    let k = format!("\"{key}\"");
    let i = s.find(&k).unwrap_or_else(|| panic!("manifest has no {key}"));
    let r = &s[i + k.len()..];
    let r = &r[r.find(':').expect("manifest colon") + 1..];
    let r = &r[r.find('"').expect("manifest quote") + 1..];
    let v = &r[..r.find('"').expect("manifest quote")];
    parse_ts(v.as_bytes()).unwrap_or_else(|| panic!("bad manifest {key}: {v}"))
}


// ------------------------------------------------------------------ fast scanner

#[cfg(test)]
const LO: u64 = 0x0101_0101_0101_0101;
#[cfg(test)]
const HI: u64 = 0x8080_8080_8080_8080;

/// High bit of each byte of the result is set exactly where `w` has byte `c`.
#[cfg(test)]
#[inline(always)]
fn eq_mask(w: u64, c: u8) -> u64 {
    let x = w ^ (LO * c as u64);
    let nonzero = ((x & !HI).wrapping_add(!HI)) | x;
    !nonzero & HI
}

/// Calls `f` with the N fields of every well-formed line in `chunk`, quotes
/// removed. Same rules as `for_lines` + `split`, but scans 8 bytes at a time.
#[inline]
#[cfg(test)]
pub fn scan_ref<const N: usize>(chunk: Bytes, mut f: impl FnMut([Bytes; N])) {
    let mut fields: [Bytes; N] = [&[]; N];
    let mut k = 0usize; // commas seen in this line
    let mut field_start = 0usize;
    let mut quoted = 0u32; // bit i: field i holds a quote
    let len = chunk.len();

    let mut on = |c: u8, pos: usize, fields: &mut [Bytes; N], k: &mut usize, field_start: &mut usize, quoted: &mut u32| match c {
        b',' => {
            if *k < N - 1 {
                fields[*k] = &chunk[*field_start..pos];
            }
            *k += 1;
            *field_start = pos + 1;
        }
        b'"' => *quoted |= 1 << (*k).min(31),
        _ => {
            // newline: finish the line
            if *k == N - 1 {
                fields[N - 1] = &chunk[*field_start..pos];
                let mut ok = true;
                if *quoted != 0 {
                    for (i, fl) in fields.iter_mut().enumerate() {
                        if *quoted & (1 << i) != 0 {
                            match unquote(fl) {
                                Some(u) => *fl = u,
                                None => {
                                    ok = false;
                                    break;
                                }
                            }
                        }
                    }
                }
                if ok {
                    f(*fields);
                }
            }
            *k = 0;
            *field_start = pos + 1;
            *quoted = 0;
        }
    };

    let mut i = 0;
    while i + 8 <= len {
        let w = u64::from_le_bytes(chunk[i..i + 8].try_into().unwrap());
        let mut m = eq_mask(w, b',') | eq_mask(w, b'\n') | eq_mask(w, b'"');
        while m != 0 {
            let pos = i + (m.trailing_zeros() / 8) as usize;
            m &= m - 1;
            on(chunk[pos], pos, &mut fields, &mut k, &mut field_start, &mut quoted);
        }
        i += 8;
    }
    while i < len {
        let c = chunk[i];
        if c == b',' || c == b'\n' || c == b'"' {
            on(c, i, &mut fields, &mut k, &mut field_start, &mut quoted);
        }
        i += 1;
    }
    // a last line without a trailing newline (it may end in an empty field)
    if field_start < len || k > 0 {
        on(b'\n', len, &mut fields, &mut k, &mut field_start, &mut quoted);
    }
}

// ------------------------------------------------------------------ fast timestamps

/// Remembers recently parsed dates ("YYYY-MM-DD" -> epoch ms of that midnight,
/// or i64::MIN for an impossible date). All valid timestamps in a dataset fall
/// on a few dozen dates, so almost every lookup hits.
pub struct DateCache {
    keys: [u128; 64],
    vals: [i64; 64],
}

impl DateCache {
    pub fn new() -> Self {
        DateCache { keys: [u128::MAX; 64], vals: [0; 64] }
    }

    #[inline(always)]
    fn midnight(&mut self, b: &[u8]) -> Option<i64> {
        let lo = u64::from_le_bytes(b[0..8].try_into().unwrap());
        let hi = u16::from_le_bytes(b[8..10].try_into().unwrap()) as u64;
        let k = (lo as u128) | ((hi as u128) << 64);
        let slot = ((lo ^ (hi << 23)).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 58) as usize;
        let v = if self.keys[slot] == k {
            self.vals[slot]
        } else {
            let v = date_slow(b).unwrap_or(i64::MIN);
            self.keys[slot] = k;
            self.vals[slot] = v;
            v
        };
        if v == i64::MIN { None } else { Some(v) }
    }
}

#[cold]
fn date_slow(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let y = d2(b, 0)? * 100 + d2(b, 2)?;
    let mo = d2(b, 5)?;
    let d = d2(b, 8)?;
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) {
        return None;
    }
    Some(days_from_civil(y, mo, d) * 86_400_000)
}

/// "HH:MM:SS" (8 bytes) to milliseconds since midnight, checked.
#[inline(always)]
fn hms(b: &[u8]) -> Option<i64> {
    let w = u64::from_le_bytes(b[0..8].try_into().unwrap());
    const COLONS: u64 = 0x0000_FF00_00FF_0000; // bytes 2 and 5
    if w & COLONS != 0x0000_3A00_003A_0000 {
        return None;
    }
    let w = (w & !COLONS) | 0x0000_3000_0030_0000; // colons -> '0'
    // all eight bytes ASCII digits?
    if (w & 0xF0F0_F0F0_F0F0_F0F0) | (((w.wrapping_add(0x0606_0606_0606_0606)) & 0xF0F0_F0F0_F0F0_F0F0) >> 4)
        != 0x3333_3333_3333_3333
    {
        return None;
    }
    let d = w - 0x3030_3030_3030_3030;
    let t = d * 10 + (d >> 8); // byte j = 10 * digit j + digit j+1
    let h = (t & 0xFF) as i64;
    let m = ((t >> 24) & 0xFF) as i64;
    let s = ((t >> 48) & 0xFF) as i64;
    if h > 23 || m > 59 || s > 59 {
        return None;
    }
    Some((h * 3600 + m * 60 + s) * 1000)
}

/// Same result as `parse_ts`, faster.
#[inline(always)]
pub fn parse_ts_fast(b: &[u8], cache: &mut DateCache) -> Option<i64> {
    let n = b.len();
    if (n == 20 || n == 25) && b[4] == b'-' {
        if b[10] != b'T' {
            return None;
        }
        let t = cache.midnight(&b[0..10])? + hms(&b[11..19])?;
        if n == 20 {
            return if b[19] == b'Z' { Some(t) } else { None };
        }
        let sign = match b[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if b[22] != b':' {
            return None;
        }
        let oh = d2(b, 20)?;
        let om = d2(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        return Some(t - sign * (oh * 60 + om) * 60_000);
    }
    if n == 0 {
        return None;
    }
    if n <= 18 {
        let mut v: i64 = 0;
        for &c in b {
            let x = c.wrapping_sub(b'0');
            if x > 9 {
                return None;
            }
            v = v * 10 + x as i64;
        }
        return Some(v);
    }
    parse_ts(b)
}

// ------------------------------------------------------------------ SIMD scanner

/// Bitmasks of one 64-byte block: bit i is set when byte i is a comma,
/// a newline, or a double quote.
#[inline(always)]
fn block_masks(p: &[u8; 64]) -> (u64, u64, u64) {
    #[cfg(target_arch = "aarch64")]
    unsafe {
        use std::arch::aarch64::*;
        const BITS: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
        let bits = vld1q_u8(BITS.as_ptr());
        let v = [vld1q_u8(p.as_ptr()), vld1q_u8(p.as_ptr().add(16)), vld1q_u8(p.as_ptr().add(32)), vld1q_u8(p.as_ptr().add(48))];
        let mask = |c: u8| -> u64 {
            let c = vdupq_n_u8(c);
            let t0 = vandq_u8(vceqq_u8(v[0], c), bits);
            let t1 = vandq_u8(vceqq_u8(v[1], c), bits);
            let t2 = vandq_u8(vceqq_u8(v[2], c), bits);
            let t3 = vandq_u8(vceqq_u8(v[3], c), bits);
            let s0 = vpaddq_u8(t0, t1);
            let s1 = vpaddq_u8(t2, t3);
            let s = vpaddq_u8(s0, s1);
            let s = vpaddq_u8(s, s);
            vgetq_lane_u64(vreinterpretq_u64_u8(s), 0)
        };
        (mask(b','), mask(b'\n'), mask(b'"'))
    }
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let a = _mm256_loadu_si256(p.as_ptr() as *const __m256i);
        let b = _mm256_loadu_si256(p.as_ptr().add(32) as *const __m256i);
        let mask = |c: u8| -> u64 {
            let c = _mm256_set1_epi8(c as i8);
            let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, c)) as u32 as u64;
            let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, c)) as u32 as u64;
            lo | (hi << 32)
        };
        (mask(b','), mask(b'\n'), mask(b'"'))
    }
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
    unsafe {
        use std::arch::x86_64::*;
        let v = [
            _mm_loadu_si128(p.as_ptr() as *const __m128i),
            _mm_loadu_si128(p.as_ptr().add(16) as *const __m128i),
            _mm_loadu_si128(p.as_ptr().add(32) as *const __m128i),
            _mm_loadu_si128(p.as_ptr().add(48) as *const __m128i),
        ];
        let mask = |c: u8| -> u64 {
            let c = _mm_set1_epi8(c as i8);
            let mut m = 0u64;
            for (i, x) in v.iter().enumerate() {
                m |= (_mm_movemask_epi8(_mm_cmpeq_epi8(*x, c)) as u16 as u64) << (16 * i);
            }
            m
        };
        (mask(b','), mask(b'\n'), mask(b'"'))
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        let (mut c, mut n, mut q) = (0u64, 0u64, 0u64);
        for (i, &x) in p.iter().enumerate() {
            c |= ((x == b',') as u64) << i;
            n |= ((x == b'\n') as u64) << i;
            q |= ((x == b'"') as u64) << i;
        }
        (c, n, q)
    }
}

/// Calls `f` with the N fields of every well-formed line in `chunk`, quotes
/// removed. Same result as `scan_ref`; finds commas, newlines and quotes 64
/// bytes at a time. Quotes are not visited one by one: a line's quotes are
/// valid exactly when every quote is the first or last byte of a field that
/// starts and ends with one, which we check by counting.
#[inline]
pub fn scan<'a, const N: usize>(chunk: &'a [u8], mut f: impl FnMut([&'a [u8]; N])) {
    let len = chunk.len();
    let mut fields: [&'a [u8]; N] = [&[]; N];
    let mut k = 0usize; // commas seen in this line
    let mut field_start = 0usize;
    let mut q_before = 0u32; // quotes before the current block
    let mut line_q = 0u32; // quotes before the current line start

    let mut base = 0usize;
    let mut pad = [0u8; 64];
    while base < len {
        let block: &[u8; 64] = if base + 64 <= len {
            chunk[base..base + 64].try_into().unwrap()
        } else {
            pad.fill(0);
            pad[..len - base].copy_from_slice(&chunk[base..]);
            &pad
        };
        let (cm, nl, qm) = block_masks(block);
        let mut m = cm | nl;
        while m != 0 {
            let bit = m.trailing_zeros();
            m &= m - 1;
            let pos = base + bit as usize;
            if nl & (1u64 << bit) == 0 {
                if k < N - 1 {
                    fields[k] = &chunk[field_start..pos];
                }
                k += 1;
                field_start = pos + 1;
            } else {
                let q_here = q_before + (qm & ((1u64 << bit) - 1)).count_ones();
                if k == N - 1 {
                    fields[N - 1] = &chunk[field_start..pos];
                    let quotes = q_here - line_q;
                    if quotes == 0 || unquote_all(&mut fields, quotes) {
                        f(fields);
                    }
                }
                line_q = q_here;
                k = 0;
                field_start = pos + 1;
            }
        }
        q_before += qm.count_ones();
        base += 64;
    }
    // last line without a trailing newline
    if k == N - 1 {
        fields[N - 1] = &chunk[field_start..len];
        let quotes = q_before - line_q;
        if quotes == 0 || unquote_all(&mut fields, quotes) {
            f(fields);
        }
    }
}

/// The line holds `quotes` quote characters. Strip the quotes around fields;
/// false if any quote is anywhere else.
#[inline]
fn unquote_all<const N: usize>(fields: &mut [&[u8]; N], quotes: u32) -> bool {
    // branch-free: about a fifth of fields are quoted, in no pattern
    let mut wrapped = 0u32;
    for fl in fields.iter_mut() {
        let n = fl.len();
        let a = fl.first().copied().unwrap_or(0);
        let z = fl.last().copied().unwrap_or(0);
        let w = ((n >= 2) & (a == b'"') & (z == b'"')) as usize;
        // SAFETY: w == 1 only when n >= 2, so w <= n - w.
        *fl = unsafe { fl.get_unchecked(w..n - w) };
        wrapped += 2 * w as u32;
    }
    wrapped == quotes
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn pick<'a>(&mut self, v: &'a [u8]) -> u8 {
            v[(self.next() % v.len() as u64) as usize]
        }
    }

    /// The fast timestamp parser must agree with the plain one on every input.
    #[test]
    fn fast_ts_matches_plain() {
        let seeds: [&[u8]; 8] = [
            b"2026-10-03T23:58:10Z",
            b"2028-02-29T00:00:00Z",
            b"2027-02-28T23:59:59Z",
            b"2026-10-03T19:58:10-04:00",
            b"2026-10-04T01:30:00+02:00",
            b"2026-10-04T01:30:00+23:59",
            b"1791071890000",
            b"0001791071890000",
        ];
        let alphabet = b"0123456789-:TZ+ tz.5913\xd9";
        let mut rng = Rng(0x1234_5678_9abc_def1);
        let mut cache = DateCache::new();
        for i in 0..3_000_000u64 {
            let mut v = seeds[(i % seeds.len() as u64) as usize].to_vec();
            let edits = rng.next() % 4;
            for _ in 0..edits {
                let pos = (rng.next() % v.len() as u64) as usize;
                match rng.next() % 6 {
                    0 if v.len() > 1 => {
                        v.remove(pos);
                    }
                    1 => v.insert(pos, rng.pick(alphabet)),
                    _ => v[pos] = rng.pick(alphabet),
                }
            }
            let b: &[u8] = &v;
            assert_eq!(parse_ts_fast(b, &mut cache), parse_ts(b), "input {:?}", String::from_utf8_lossy(b));
        }
        // every day of 2024..2029 and some impossible ones, every hour/minute edge
        for y in 2024..2030 {
            for m in 0..14 {
                for d in 0..33 {
                    for (h, mi, s) in [(0, 0, 0), (23, 59, 59), (24, 0, 0), (12, 60, 0), (12, 0, 60), (9, 9, 9)] {
                        let t = format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z");
                        assert_eq!(parse_ts_fast(t.as_bytes(), &mut cache), parse_ts(t.as_bytes()), "{t}");
                    }
                }
            }
        }
    }

    /// Pieces of any size hand out every line after the header exactly once.
    #[test]
    fn pieces_cover_every_line_once() {
        let dir = std::env::temp_dir().join(format!("pieces-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut rng = Rng(0x5eed_0f_7e57_0001);
        let mut paths = Vec::new();
        let mut want = Vec::new();
        for f in 0..12 {
            let mut v: Vec<u8> = Vec::new();
            let kind = f % 4;
            if kind != 0 {
                v.extend_from_slice(b"h1,h2\n");
            }
            if kind == 0 && f > 0 {
                v.extend_from_slice(b"only,header"); // no newline at all
            }
            let lines = if kind == 1 { 0 } else { rng.next() % 300 };
            for i in 0..lines {
                let w = (rng.next() % 40) as usize;
                let line: Vec<u8> = (0..w).map(|_| rng.pick(b"ab,\"\r xyz")).collect();
                v.extend_from_slice(&line);
                if i + 1 < lines || rng.next() % 2 == 0 {
                    v.push(b'\n');
                }
            }
            want.extend_from_slice(body(Box::leak(v.clone().into_boxed_slice())));
            if !want.is_empty() && *want.last().unwrap() != b'\n' {
                want.push(b'\n');
            }
            let p = dir.join(format!("f{f:02}.csv"));
            std::fs::write(&p, &v).unwrap();
            paths.push(p);
        }
        for chunk in [1u64, 2, 3, 5, 8, 13, 64, 100, 1000, 100_000] {
            let pieces = Pieces::plan_with(&paths, chunk);
            let mut got = Vec::new();
            let mut buf = Vec::new();
            for i in 0..pieces.pieces.len() {
                let r = pieces.read(i, &mut buf);
                assert!(buf.len() >= r.end + 64);
                got.extend_from_slice(&buf[r]);
                if !got.is_empty() && *got.last().unwrap() != b'\n' {
                    got.push(b'\n');
                }
            }
            assert_eq!(String::from_utf8_lossy(&got), String::from_utf8_lossy(&want), "chunk {chunk}");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    fn collect<const N: usize>(chunk: Bytes, fast: bool) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        let mut push = |f: [Bytes; N]| out.push(f.iter().map(|x| x.to_vec()).collect());
        if fast { scan::<N>(chunk, &mut push) } else { scan_ref::<N>(chunk, &mut push) }
        out
    }

    /// The SIMD scanner must produce exactly the same lines and fields.
    #[test]
    fn scan_matches_reference() {
        let alphabet = b"ab,,,,\"\"\n\n\r x1\xd9";
        let mut rng = Rng(0xfeed_beef_1234_5678);
        for round in 0..20_000 {
            let len = (rng.next() % 300) as usize;
            let mut v: Vec<u8> = Vec::with_capacity(len);
            for _ in 0..len {
                // mostly well-formed-ish lines: favour letters, commas, quotes around fields
                v.push(rng.pick(alphabet));
            }
            if round % 3 == 0 {
                // a block of valid quoted lines too
                for i in 0..(rng.next() % 40) {
                    let line = format!("\"e{i}\",u{i},\"\",x,\"y\",p{i},{}\n", i * 7);
                    v.extend_from_slice(line.as_bytes());
                }
            }
            let chunk: Bytes = Box::leak(v.into_boxed_slice());
            assert_eq!(collect::<2>(chunk, true), collect::<2>(chunk, false), "N=2 {:?}", String::from_utf8_lossy(chunk));
            assert_eq!(collect::<4>(chunk, true), collect::<4>(chunk, false), "N=4 {:?}", String::from_utf8_lossy(chunk));
            assert_eq!(collect::<7>(chunk, true), collect::<7>(chunk, false), "N=7 {:?}", String::from_utf8_lossy(chunk));
        }
    }

    #[test]
    fn key_fast_matches_key() {
        let buf: &'static [u8] = b"u1, U1,u1 ,abcdefghijklmno,abcdefghijklmnop,,x\xd9\xa2,p42104,                ";
        let end = buf.as_ptr_range().end;
        for f in buf.split(|&c| c == b',') {
            assert_eq!(key_fast(f, end), key(f));
            assert_eq!(key_bytes(key(f)), f.to_vec());
        }
    }

    #[test]
    fn key_order_is_byte_order() {
        let words: [&'static [u8]; 9] = [b"", b"a", b"a\x00", b"ab", b"b", b"U1", b"u1", b" u1", b"abcdefghijklmnopq"];
        for a in words {
            for b in words {
                assert_eq!(key_cmp(key(a), key(b)), a.cmp(b), "{:?} {:?}", a, b);
            }
        }
    }
}
