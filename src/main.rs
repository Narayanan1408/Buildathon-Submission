//! Shopping Sessions pipeline. Implements rules.md exactly.
//!
//! Plan (every step runs on all CPU cores):
//!   1. Read products, campaigns, manifest. Build the linked-ID map (union-find)
//!      on a side thread while events are being parsed.
//!   2. Parse every event line; send each copy to a bucket by hash(event_id).
//!   3. Per bucket: dedup on event_id (smallest ingest_ts), keep valid survivors,
//!      re-route each survivor to a bucket by hash(person name).
//!      In the same pass: parse + validate clicks, route by hash(person name).
//!   4. Per person bucket: sessionize, credit, assists, user_daily rows.
//!   5. Write Parquet: one user_daily file per worker, one campaign_daily file.
//!
//! Buckets are small (NB = 1024) so each one's hash maps and sorts stay in
//! the CPU cache.

mod out;
mod seg;
mod text;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fs;
use std::hash::BuildHasher;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use foldhash::fast::FixedState as RandomState;

use out::{NO_TOP, UserRows, write_campaign_daily, write_user_daily};
use seg::{SegList, SegPool};
use text::*;

type Map<K, V> = HashMap<K, V, RandomState>;

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const NB_BITS: u32 = 10;
const NB: usize = 1 << NB_BITS;

const VIEW: u8 = 0;
const CART: u8 = 1;
const PURCHASE: u8 = 2;
const INVALID: u8 = u8::MAX;

/// Hasher for routing rows to buckets. Different seed from the map hasher so
/// that keys inside one bucket still spread well inside its hash map.
fn route_hasher() -> RandomState {
    RandomState::with_seed(0x243f6a8885a308d3)
}
fn map_hasher() -> RandomState {
    RandomState::with_seed(0x452821e638d01377)
}
fn new_map<K, V>(cap: usize) -> Map<K, V> {
    HashMap::with_capacity_and_hasher(cap, map_hasher())
}
#[inline]
fn bucket(h: &RandomState, k: Key) -> usize {
    (h.hash_one(k) >> (64 - NB_BITS)) as usize
}

fn new_lists<T: Copy>() -> Vec<SegList<T>> {
    (0..NB).map(|_| SegList::new()).collect()
}

/// Run `work(state, item)` for items 0..n on `threads` threads, each with its
/// own state (made by `init(thread index)`); items are handed out in order.
fn workers<S: Send>(
    threads: usize,
    n: usize,
    init: impl Fn(usize) -> S + Sync,
    work: impl Fn(&mut S, usize) + Sync,
) -> Vec<S> {
    let next = AtomicUsize::new(0);
    std::thread::scope(|sc| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let init = &init;
                let work = &work;
                let next = &next;
                sc.spawn(move || {
                    let mut s = init(t);
                    loop {
                        let i = next.fetch_add(1, AtomicOrdering::Relaxed);
                        if i >= n {
                            break;
                        }
                        work(&mut s, i);
                    }
                    s
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().expect("worker panicked")).collect()
    })
}

// ------------------------------------------------------------------ linked IDs

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    let mut root = x;
    while parent[root as usize] != root {
        root = parent[root as usize];
    }
    while parent[x as usize] != root {
        let next = parent[x as usize];
        parent[x as usize] = root;
        x = next;
    }
    root
}

/// Map from every linked ID to its person's name (the byte-wise smallest ID),
/// split into NB maps by bucket(id).
fn build_links(files: &[PathBuf]) -> Vec<Map<Key, Key>> {
    let mut pairs: Vec<(Key, Key)> = Vec::new();
    for c in chunks(files) {
        for_lines(c, |l| {
            if let Some([a, b]) = split::<2>(l) {
                if !a.is_empty() && !b.is_empty() {
                    pairs.push((key(a), key(b)));
                }
            }
        });
    }
    let mut index: Map<Key, u32> = new_map(pairs.len() * 2);
    let mut ids: Vec<Key> = Vec::with_capacity(pairs.len() * 2);
    let mut parent: Vec<u32> = Vec::with_capacity(pairs.len() * 2);
    let mut size: Vec<u32> = Vec::with_capacity(pairs.len() * 2);
    for &(a, b) in &pairs {
        let mut intern = |s: Key| -> u32 {
            *index.entry(s).or_insert_with(|| {
                let i = ids.len() as u32;
                ids.push(s);
                parent.push(i);
                size.push(1);
                i
            })
        };
        let ia = intern(a);
        let ib = intern(b);
        let ra = find(&mut parent, ia);
        let rb = find(&mut parent, ib);
        if ra != rb {
            let (big, small) = if size[ra as usize] >= size[rb as usize] { (ra, rb) } else { (rb, ra) };
            parent[small as usize] = big;
            size[big as usize] += size[small as usize];
        }
    }
    let n = ids.len();
    let mut root_name: Vec<Key> = ids.clone();
    let mut roots = vec![0u32; n];
    for i in 0..n {
        let r = find(&mut parent, i as u32);
        roots[i] = r;
        if key_cmp(ids[i], root_name[r as usize]) == Ordering::Less {
            root_name[r as usize] = ids[i];
        }
    }
    // split by bucket(id), so each user bucket only needs its own small map
    let rh = route_hasher();
    let mut out: Vec<Map<Key, Key>> = (0..NB).map(|_| new_map(n / NB + 8)).collect();
    for i in 0..n {
        out[bucket(&rh, ids[i])].insert(ids[i], root_name[roots[i] as usize]);
    }
    out
}

// ------------------------------------------------------------------ records

/// One copy of an event, for dedup only: the address of its full record
/// (`loc`, or NO_LOC when the copy is invalid and has no record).
#[derive(Clone, Copy)]
#[repr(C, packed(8))]
struct Dd {
    eid: Key,
    ingest: i64,
    loc: u64,
}
const NO_LOC: u64 = u64::MAX;

/// by_thread[t][b] -> out[b][t], each bucket's lists behind their own lock so
/// that the worker handling bucket b can take (and then recycle) them.
fn transpose<T>(by_thread: Vec<Vec<T>>) -> Vec<std::sync::Mutex<Vec<T>>> {
    let mut out: Vec<Vec<T>> = (0..NB).map(|_| Vec::with_capacity(by_thread.len())).collect();
    for per_bucket in by_thread {
        for (b, v) in per_bucket.into_iter().enumerate() {
            out[b].push(v);
        }
    }
    out.into_iter().map(std::sync::Mutex::new).collect()
}

/// One parse worker's output: valid event copies by bucket(user ID) (their
/// `name` is still the user ID), and dedup notes for every copy by bucket(event_id).
struct Parsed {
    evu: Vec<SegList<EvP>>,
    dd: Vec<SegList<Dd>>,
    buf: Vec<u8>,
}

#[derive(Clone, Copy)]
#[repr(C, packed(8))]
struct EvP {
    name: Key,
    ts: i64,
    price: u32,
    cat: u16,
    typ: u8,
}

#[derive(Clone, Copy)]
struct Ck {
    name: Key,
    cid: Key,
    ts: i64,
    camp: u32,
}

#[derive(Clone, Copy, Default)]
struct EvL {
    ts: i64,
    price: u32,
    cat: u16,
    typ: u8,
}

#[derive(Clone, Copy, Default)]
struct CkL {
    p: u32,
    camp: u32,
    ts: i64,
    cid: Key,
}

struct Sess {
    start: i64,
    di: u32, // day - first window day
    revenue: i64,
    purchased: bool,
}

/// One worker's output from the dedup/click pass, by person bucket.
struct Routed {
    evp: Vec<SegList<EvP>>,
    ck: Vec<SegList<Ck>>,
}

/// Per (campaign, day): credited sessions, purchasing sessions, revenue.
#[derive(Clone, Copy, Default)]
struct Credit {
    sessions: u32,
    purchasing: u32,
    revenue: i64,
}

/// One worker's campaign totals; index campaign * nd + day.
struct CampAcc {
    credit: Vec<Credit>,
    assisted: Vec<u32>,
}

struct Window {
    ws: i64,
    we: i64,
    wsd: i64,
    nd: usize,
}

/// Current physical memory footprint in MB (macOS only; 0 elsewhere). For TIMING output.
fn footprint_mb() -> u64 {
    #[cfg(target_os = "macos")]
    unsafe {
        let mut info: libc::rusage_info_v4 = std::mem::zeroed();
        if libc::proc_pid_rusage(libc::getpid(), libc::RUSAGE_INFO_V4, &mut info as *mut _ as *mut libc::rusage_info_t) == 0 {
            return info.ri_phys_footprint >> 20;
        }
    }
    0
}

/// User + system CPU time of this process so far, in seconds.
fn cpu_seconds() -> f64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data = std::env::var("DATA_DIR").ok().or_else(|| args.get(1).cloned()).unwrap_or("/data".into());
    let out = std::env::var("OUTPUT_DIR").ok().or_else(|| args.get(2).cloned()).unwrap_or("/output".into());
    let data = PathBuf::from(data);
    let out = PathBuf::from(out);
    let threads = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(4, |n| n.get()))
        .max(1);
    let timing = std::env::var("TIMING").is_ok();
    let t0 = std::time::Instant::now();
    let last_cpu = std::cell::Cell::new(0.0f64);
    let lap = |what: &str| {
        if timing {
            let c = cpu_seconds();
            eprintln!("{:>8.1} ms  cpu +{:>6.0} ms  mem {:>6} MB  {what}", t0.elapsed().as_secs_f64() * 1000.0, (c - last_cpu.get()) * 1000.0, footprint_mb());
            last_cpu.set(c);
        }
    };

    // ---- small inputs
    let manifest = fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let ws = manifest_value(&manifest, "window_start");
    let we = manifest_value(&manifest, "window_end");
    let wsd = ws.div_euclid(DAY);
    let nd = ((we - 1).div_euclid(DAY) - wsd + 1).max(1) as usize;
    let win = Window { ws, we, wsd, nd };

    let mut cat_index: Map<Bytes, u16> = new_map(64);
    let mut cat_names: Vec<Bytes> = Vec::new();
    let mut prod: Map<Key, u16> = new_map(1 << 16);
    for_lines(body(map_file(&data.join("products.csv"))), |l| {
        if let Some([p, c]) = split::<2>(l) {
            let ci = *cat_index.entry(c).or_insert_with(|| {
                cat_names.push(c);
                (cat_names.len() - 1) as u16
            });
            prod.entry(key(p)).or_insert(ci);
        }
    });
    let unknown = cat_names.len() as u16;
    cat_names.push(b"unknown");
    // rank[c] = position of category c in byte-wise order (for tie breaks)
    let mut order: Vec<u16> = (0..cat_names.len() as u16).collect();
    order.sort_by(|&a, &b| cat_names[a as usize].cmp(cat_names[b as usize]));
    let mut cat_rank = vec![0u32; cat_names.len()];
    for (r, &c) in order.iter().enumerate() {
        cat_rank[c as usize] = r as u32;
    }

    let mut camp_index: Map<Key, u32> = new_map(4096);
    let mut camp_ids: Vec<Bytes> = Vec::new();
    let mut channels: Vec<Bytes> = Vec::new();
    for_lines(body(map_file(&data.join("campaigns.csv"))), |l| {
        if let Some([c, ch]) = split::<2>(l) {
            if let Entry::Vacant(v) = camp_index.entry(key(c)) {
                v.insert(camp_ids.len() as u32);
                camp_ids.push(c);
                channels.push(ch);
            }
        }
    });
    let organic = camp_ids.len() as u32;
    camp_ids.push(b"organic");
    channels.push(b"organic");
    let ncamp = camp_ids.len();
    lap("small files");

    let rh = route_hasher();
    let link_files = csv_files(&data.join("links"));
    // All bucket lists live in pool segments: memory freed by one pass
    // (already paged in) is reused by the next.
    let seg_pool = SegPool::new();
    let ev_pieces = Pieces::plan(&csv_files(&data.join("events")));
    let ck_pieces = Pieces::plan(&csv_files(&data.join("clicks")));
    let n_ck = ck_pieces.pieces.len();

    // ---- links (side thread) + events: parse; valid copies go to their user
    //      bucket, a dedup note for every copy goes to its event_id bucket.
    let (links, parsed) = std::thread::scope(|sc| {
        let links = sc.spawn(|| build_links(&link_files));
        let parsed: Vec<Parsed> = workers(
            threads,
            ev_pieces.pieces.len(),
            |_| Parsed { evu: new_lists(), dd: new_lists(), buf: Vec::new() },
            |out, ci| {
                let mut buf = std::mem::take(&mut out.buf);
                let n = ev_pieces.read(ci, &mut buf);
                parse_events(&buf[..n], buf.as_ptr_range().end, out, &win, &prod, unknown, &rh, &seg_pool);
                out.buf = buf;
            },
        );
        (links.join().expect("links thread"), parsed)
    });
    lap("links + events parsed");

    // ---- events: per event_id bucket, find the copies that lose (a copy with
    //      a smaller ingest_ts exists) and mark them invalid in place.
    //      clicks: parse, validate, route by user ID. Same worker pool.
    struct DedupOut {
        ck: Vec<SegList<Ck>>,
        buf: Vec<u8>,
    }
    let mut ev_in = Vec::with_capacity(parsed.len());
    let mut dd_in = Vec::with_capacity(parsed.len());
    for p in parsed {
        ev_in.push(p.evu);
        dd_in.push(p.dd);
    }
    let dd_in = transpose(dd_in);
    let cws = ws - WEEK;
    let dedup: Vec<DedupOut> = workers(
        threads,
        n_ck + NB,
        |_| DedupOut { ck: new_lists(), buf: Vec::new() },
        |r, item| {
            // dedup first: the clicks then go into the recycled dedup notes
            if item >= NB {
                let mut buf = std::mem::take(&mut r.buf);
                let n = ck_pieces.read(item - NB, &mut buf);
                let end = buf.as_ptr_range().end;
                let chunk = &buf[..n];
                let mut dc = DateCache::new();
                scan::<4>(chunk, |[cid, user, camp, ts]| {
                    if cid.is_empty() || user.is_empty() {
                        return;
                    }
                    let Some(t) = parse_ts_fast(ts, &mut dc) else { return };
                    if t < cws || t >= we {
                        return;
                    }
                    let Some(&ci) = camp_index.get(&key_fast(camp, end)) else { return };
                    let user = key_fast(user, end);
                    r.ck[bucket(&rh, user)].push(&seg_pool, Ck { name: user, cid: key_fast(cid, end), ts: t, camp: ci });
                });
                r.buf = buf;
                return;
            }
            let b = item;
            let lists = std::mem::take(&mut *dd_in[b].lock().unwrap());
            let n: usize = lists.iter().map(|l| l.len()).sum();
            let mut best: Map<Key, (i64, u64)> = new_map(n);
            let lose = |l: u64| {
                if l != NO_LOC {
                    // SAFETY: l is the address of a stored EvP (see parse_events);
                    // each copy loses at most once, and no one else touches the
                    // event records during this pass.
                    unsafe { std::ptr::addr_of_mut!((*(l as *mut EvP)).typ).write(INVALID) };
                }
            };
            for d in lists.iter().flat_map(|l| l.slices()).flatten() {
                let (eid, ingest, l) = (d.eid, d.ingest, d.loc);
                match best.entry(eid) {
                    Entry::Occupied(mut o) => {
                        let cur = o.get_mut();
                        if ingest < cur.0 {
                            lose(cur.1);
                            *cur = (ingest, l);
                        } else {
                            lose(l);
                        }
                    }
                    Entry::Vacant(v) => {
                        v.insert((ingest, l));
                    }
                }
            }
            for l in lists {
                l.recycle(&seg_pool);
            }
        },
    );
    lap("events deduped, clicks parsed");

    // ---- per user bucket: replace each user ID by its person's name, using
    //      only this bucket's slice of the link map, and route by name.
    // Each input bucket's segments are recycled as soon as it has been read.
    let ev_in = transpose(ev_in);
    let ck_in = transpose(dedup.into_iter().map(|d| d.ck).collect());
    let routed: Vec<Routed> = workers(
        threads,
        NB,
        |_| Routed { evp: new_lists(), ck: new_lists() },
        |r, b| {
            let lk = &links[b];
            for v in std::mem::take(&mut *ev_in[b].lock().unwrap()) {
                for e in v.slices().flatten() {
                    if e.typ == INVALID {
                        continue;
                    }
                    let user = e.name;
                    let name = lk.get(&user).copied().unwrap_or(user);
                    r.evp[bucket(&rh, name)].push(&seg_pool, EvP { name, ..*e });
                }
                v.recycle(&seg_pool);
            }
            for v in std::mem::take(&mut *ck_in[b].lock().unwrap()) {
                for c in v.slices().flatten() {
                    let name = lk.get(&c.name).copied().unwrap_or(c.name);
                    r.ck[bucket(&rh, name)].push(&seg_pool, Ck { name, ..*c });
                }
                v.recycle(&seg_pool);
            }
        },
    );
    lap("links resolved");

    // ---- per person bucket: sessions, credit, assists
    let ud_dir = out.join("user_daily");
    let cd_dir = out.join("campaign_daily");
    fs::create_dir_all(&ud_dir).expect("create user_daily");
    fs::create_dir_all(&cd_dir).expect("create campaign_daily");

    // biggest buckets first, so no worker is left with a big one at the end
    let mut order: Vec<usize> = (0..NB).collect();
    let sizes: Vec<usize> = (0..NB).map(|b| routed.iter().map(|r| r.evp[b].len()).sum()).collect();
    order.sort_unstable_by_key(|&b| std::cmp::Reverse(sizes[b]));

    struct PersonWork {
        rows: UserRows,
        acc: CampAcc,
        scratch: Scratch,
    }
    let ctx = Ctx { win: &win, cat_rank: &cat_rank, organic };
    let done: Vec<PersonWork> = workers(
        threads,
        NB,
        |_| PersonWork {
            rows: UserRows::default(),
            acc: CampAcc { credit: vec![Credit::default(); ncamp * nd], assisted: vec![0; ncamp * nd] },
            scratch: Scratch::default(),
        },
        |w, i| {
            let b = order[i];
            person_bucket(&ctx, &routed, b, sizes[b], &mut w.scratch, &mut w.rows, &mut w.acc);
        },
    );
    lap("sessions + attribution");

    let mut total: Vec<[i64; 4]> = vec![[0; 4]; ncamp * nd];
    let mut user_parts = Vec::new();
    for w in done {
        for (i, t) in total.iter_mut().enumerate() {
            let c = w.acc.credit[i];
            t[0] += c.sessions as i64;
            t[1] += c.purchasing as i64;
            t[2] += c.revenue;
            t[3] += w.acc.assisted[i] as i64;
        }
        user_parts.push(w.rows);
    }
    let any_rows = user_parts.iter().any(|r| r.len() > 0);
    std::thread::scope(|sc| {
        for (i, rows) in user_parts.into_iter().enumerate() {
            if rows.len() > 0 || (i == 0 && !any_rows) {
                let path = ud_dir.join(format!("part-{i:03}.parquet"));
                let cat_names = &cat_names;
                sc.spawn(move || write_user_daily(&path, rows, cat_names));
            }
        }
        write_campaign_daily(&cd_dir.join("part-000.parquet"), &total, &camp_ids, &channels, nd, wsd);
    });
    lap("parquet written");
    // Skip freeing memory and unmapping files: the OS does it faster at exit.
    std::process::exit(0);
}

#[inline]
#[allow(clippy::too_many_arguments)]
fn parse_events(
    chunk: &[u8],
    end: *const u8,
    out: &mut Parsed,
    win: &Window,
    prod: &Map<Key, u16>,
    unknown: u16,
    rh: &RandomState,
    pool: &SegPool,
) {
    let (ws, we) = (win.ws, win.we);
    let mut dc = DateCache::new();
    scan::<7>(chunk, |[eid, user, ts, ing, typ, product, price]| {
        if eid.is_empty() {
            return;
        }
        let eid = key_fast(eid, end);
        let ingest = parse_ts_fast(ing, &mut dc).unwrap_or(i64::MAX);
        let mut typ = match typ {
            b"view" => VIEW,
            b"cart" => CART,
            b"purchase" => PURCHASE,
            _ => INVALID,
        };
        if user.is_empty() || product.is_empty() {
            typ = INVALID;
        }
        let t = match parse_ts_fast(ts, &mut dc) {
            Some(t) if t >= ws && t < we => t,
            _ => {
                typ = INVALID;
                0
            }
        };
        let mut p = 0;
        if typ == PURCHASE {
            match parse_price(price) {
                Some(v) => p = v,
                None => typ = INVALID,
            }
        }
        if typ != INVALID && ingest.saturating_sub(t) > DAY {
            typ = INVALID;
        }
        let mut l = NO_LOC;
        if typ != INVALID {
            // only purchases need a category: it decides top_category
            let cat = if typ == PURCHASE { prod.get(&key_fast(product, end)).copied().unwrap_or(unknown) } else { 0 };
            let user = key_fast(user, end);
            l = out.evu[bucket(rh, user)].push(pool, EvP { name: user, ts: t, price: p, cat, typ }) as u64;
        }
        out.dd[bucket(rh, eid)].push(pool, Dd { eid, ingest, loc: l });
    });
}

struct Ctx<'a> {
    win: &'a Window,
    cat_rank: &'a [u32],
    organic: u32,
}

#[derive(Default)]
struct Scratch {
    pidx: Map<Key, u32>,
    names: Vec<Key>,
    ev_p: Vec<u32>,
    ev_off: Vec<usize>,
    cur: Vec<usize>,
    evs: Vec<EvL>,
    ck_tmp: Vec<CkL>,
    ck_off: Vec<usize>,
    cks: Vec<CkL>,
    pcv: Vec<CkL>,
    sess: Vec<Sess>,
    cat_sum: Vec<(u16, i64)>,
    cc: Vec<u64>,
}

#[allow(clippy::too_many_arguments)]
/// off[p]..off[p+1] is where person p's items go; cur starts as a copy of off.
fn counting_offsets(ps: &[u32], np: usize, off: &mut Vec<usize>, cur: &mut Vec<usize>) {
    off.clear();
    off.resize(np + 1, 0);
    for &p in ps {
        off[p as usize + 1] += 1;
    }
    for i in 0..np {
        off[i + 1] += off[i];
    }
    cur.clear();
    cur.extend_from_slice(&off[..np]);
}

#[allow(clippy::too_many_arguments)]
fn person_bucket(
    ctx: &Ctx,
    routed: &[Routed],
    b: usize,
    n: usize,
    s: &mut Scratch,
    rows: &mut UserRows,
    acc: &mut CampAcc,
) {
    let cws = ctx.win.ws - WEEK;
    let Window { wsd, nd, .. } = *ctx.win;
    s.pidx.clear();
    s.pidx.reserve(n / 2 + 16);
    s.names.clear();
    s.ev_p.clear();
    s.ck_tmp.clear();

    // person index of every event
    for e in routed.iter().flat_map(|r| r.evp[b].slices()).flatten() {
        {
            let names = &mut s.names;
            let p = *s.pidx.entry(e.name).or_insert_with(|| {
                names.push(e.name);
                (names.len() - 1) as u32
            });
            s.ev_p.push(p);
        }
    }
    let np = s.names.len();

    // group events by person (counting sort), then sort each person's by time
    counting_offsets(&s.ev_p, np, &mut s.ev_off, &mut s.cur);
    s.evs.clear();
    s.evs.resize(s.ev_p.len(), EvL::default());
    let mut k = 0;
    for e in routed.iter().flat_map(|r| r.evp[b].slices()).flatten() {
        {
            let p = s.ev_p[k] as usize;
            k += 1;
            s.evs[s.cur[p]] = EvL { ts: e.ts, price: e.price, cat: e.cat, typ: e.typ };
            s.cur[p] += 1;
        }
    }
    for p in 0..np {
        let sl = &mut s.evs[s.ev_off[p]..s.ev_off[p + 1]];
        if sl.len() > 1 {
            sl.sort_unstable_by_key(|e| e.ts);
        }
    }

    // clicks of the people in this bucket, grouped by person the same way
    for c in routed.iter().flat_map(|r| r.ck[b].slices()).flatten() {
        {
            if let Some(&p) = s.pidx.get(&c.name) {
                s.ck_tmp.push(CkL { p, camp: c.camp, ts: c.ts, cid: c.cid });
            }
        }
    }
    s.ev_p.clear();
    s.ev_p.extend(s.ck_tmp.iter().map(|c| c.p));
    counting_offsets(&s.ev_p, np, &mut s.ck_off, &mut s.cur);
    s.cks.clear();
    s.cks.resize(s.ck_tmp.len(), CkL::default());
    for c in &s.ck_tmp {
        let p = c.p as usize;
        s.cks[s.cur[p]] = *c;
        s.cur[p] += 1;
    }

    let Scratch { names, evs, ev_off, cks, ck_off, pcv, sess, cat_sum, cc, .. } = s;
    for p in 0..np {
        let pe = &evs[ev_off[p]..ev_off[p + 1]];
        // this person's clicks by (ts, click_id), copies of a click_id dropped
        let pcr = &mut cks[ck_off[p]..ck_off[p + 1]];
        if pcr.len() > 1 {
            pcr.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| key_cmp(a.cid, b.cid)));
        }
        pcv.clear();
        for c in pcr.iter() {
            if pcv.last().is_none_or(|l: &CkL| l.cid != c.cid) {
                pcv.push(*c);
            }
        }
        let pc = &pcv[..];

        // sessions + user_daily rows
        sess.clear();
        let name = names[p as usize];
        let mut cur_day = i64::MIN;
        let (mut d_sess, mut d_ev, mut d_rev) = (0i64, 0i64, 0i64);
        cat_sum.clear();
        let flush = |rows: &mut UserRows, day: i64, ns: i64, ne: i64, nr: i64, cat_sum: &[(u16, i64)]| {
            let mut top = NO_TOP;
            let mut top_v = 0i64;
            for &(c, v) in cat_sum {
                if top == NO_TOP || v > top_v || (v == top_v && ctx.cat_rank[c as usize] < ctx.cat_rank[top as usize]) {
                    top = c;
                    top_v = v;
                }
            }
            rows.push_name(name);
            rows.day.push(day as i32);
            rows.sessions.push(ns);
            rows.events.push(ne);
            rows.revenue.push(nr);
            rows.top.push(top);
        };
        let mut k = 0;
        while k < pe.len() {
            let start = pe[k].ts;
            let day = start.div_euclid(DAY);
            if day != cur_day {
                if cur_day != i64::MIN {
                    flush(rows, cur_day, d_sess, d_ev, d_rev, cat_sum);
                }
                cur_day = day;
                d_sess = 0;
                d_ev = 0;
                d_rev = 0;
                cat_sum.clear();
            }
            let mut last = start;
            let mut rev = 0i64;
            let mut purchased = false;
            let mut m = k;
            while m < pe.len() && pe[m].ts - last <= GAP {
                let e = &pe[m];
                last = e.ts;
                if e.typ == PURCHASE {
                    rev += e.price as i64;
                    purchased = true;
                    match cat_sum.iter_mut().find(|x| x.0 == e.cat) {
                        Some(x) => x.1 += e.price as i64,
                        None => cat_sum.push((e.cat, e.price as i64)),
                    }
                }
                m += 1;
            }
            d_sess += 1;
            d_ev += (m - k) as i64;
            d_rev += rev;
            sess.push(Sess { start, di: (day - wsd) as u32, revenue: rev, purchased });
            k = m;
        }
        flush(rows, cur_day, d_sess, d_ev, d_rev, cat_sum);

        // credit: latest click in [start - 7d, start], tie smallest click_id
        let (mut lo, mut hi) = (0usize, 0usize);
        for se in sess.iter() {
            while hi < pc.len() && pc[hi].ts <= se.start {
                hi += 1;
            }
            while lo < hi && pc[lo].ts < se.start - WEEK {
                lo += 1;
            }
            let camp = if lo < hi {
                let mt = pc[hi - 1].ts;
                let mut q = hi - 1;
                while q > lo && pc[q - 1].ts == mt {
                    q -= 1;
                }
                pc[q].camp
            } else {
                ctx.organic
            };
            let a = &mut acc.credit[camp as usize * nd + se.di as usize];
            a.sessions += 1;
            a.purchasing += se.purchased as u32;
            a.revenue += se.revenue;
        }

        // assists: per campaign, merge [click, click + 7d] intervals and
        // count session starts inside them, per day
        if !pc.is_empty() {
            // (campaign, click time) packed in one number: sorting it groups
            // each campaign's clicks in time order
            cc.clear();
            cc.extend(pc.iter().map(|c| ((c.camp as u64) << 40) | (c.ts - cws) as u64));
            cc.sort_unstable();
            // sessions are sorted by start; each person has only a few, so
            // walking them is cheaper than binary searching per day
            let assisted = &mut acc.assisted;
            let mut emit = |camp: u64, a: i64, b: i64| {
                let mut i = sess.partition_point(|s| s.start < a);
                while i < sess.len() && sess[i].start <= b {
                    assisted[camp as usize * nd + sess[i].di as usize] += 1;
                    i += 1;
                }
            };
            let unpack = |v: u64| (v >> 40, (v & ((1 << 40) - 1)) as i64 + cws);
            let (mut cur_c, t0) = unpack(cc[0]);
            let (mut cur_a, mut cur_b) = (t0, t0 + WEEK);
            for &v in &cc[1..] {
                let (c, t) = unpack(v);
                if c == cur_c && t <= cur_b {
                    cur_b = cur_b.max(t + WEEK);
                } else {
                    emit(cur_c, cur_a, cur_b);
                    cur_c = c;
                    cur_a = t;
                    cur_b = t + WEEK;
                }
            }
            emit(cur_c, cur_a, cur_b);
        }
    }
}

