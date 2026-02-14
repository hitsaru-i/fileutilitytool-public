#!/usr/bin/env python3

import os
import shutil
import hashlib
import sqlite3
import threading
import time
import tkinter as tk
from tkinter import ttk
from tkinter import scrolledtext,filedialog,messagebox
from pathlib import Path
from datetime import datetime, timezone

defaultdbname = 'hashdata.sqlite'

# -------------------------
# Metadata helpers
# -------------------------

def _ensure_metadata_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()


def set_metadata(conn, key, value):
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)", (key, str(value)))
    conn.commit()


def get_metadata(conn, key, default=None):
    cur = conn.cursor()
    cur.execute("SELECT value FROM metadata WHERE key = ?", (key,))
    row = cur.fetchone()
    return row[0] if row else default

# -------------------------
# Database schema helpers
# -------------------------

def ensure_schema_for_grouper(conn):
    """Create/upgrade required tables: hashvalues (if missing fields) and filegroups."""
    cur = conn.cursor()

    # Create hashvalues table if missing
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hashvalues (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            row INTEGER,
            path TEXT UNIQUE,
            hash TEXT,
            duplicate INTEGER,
            deleted INTEGER
        )
    """)
    # indices
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hash ON hashvalues(hash)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_path ON hashvalues(path)")

    # Create filegroups table (new for grouper)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS filegroups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            originpath TEXT,
            destinationpath TEXT,
            filename TEXT,
            extension TEXT,
            hash TEXT,
            copied INTEGER DEFAULT 0,
            duplicate INTEGER,
            created_at TEXT,
            notes TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fg_hash ON filegroups(hash)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fg_origin ON filegroups(originpath)")

    # ensure metadata table
    _ensure_metadata_table(conn)
    conn.commit()



# -------------------------
# Utility helpers
# -------------------------

def compute_md5(path, blocksize=65536):
    """Return md5 hexdigest of a file, or None on error."""
    try:
        h = hashlib.md5()
        with open(path, 'rb') as fh:
            for chunk in iter(lambda: fh.read(blocksize), b''):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

def safe_makedirs(path):
    os.makedirs(path, exist_ok=True)

def safe_copy(src, dst, *, overwrite=False):
    """Copy file src->dst safely. Returns True if copied, False if skipped."""
    try:
        parent = os.path.dirname(dst)
        if parent:
            safe_makedirs(parent)
        if os.path.exists(dst) and not overwrite:
            return False
        shutil.copy2(src, dst)
        return True
    except Exception:
        return False


def get_extension_variant(path, mode='ext_with_dot'):
    """Return extension key according to grouping_mode.

    mode options:
        - 'ext': extension without dot (e.g. 'jpg')
        - 'ext_with_dot': prefix with 'dot ' for hidden-like files (e.g. 'dot bashrc' or 'dot txt')
        - 'filename': basename without extension (for grouping by filename)
    """
    p = Path(path)
    if mode == 'filename':
        return p.stem if p.stem else 'Miscellaneous'
    ext = p.suffix.lower()
    if ext == '':
        return 'Miscellaneous'
    if mode == 'ext':
        return ext.lstrip('.')
    # default ext_with_dot
    return 'dot ' + ext.lstrip('.')



# -------------------------
# filegroups helpers
# -------------------------

def insert_filegroup_entry(conn, originpath, filename, extension, filehash=None):
    cur = conn.cursor()
    # check if originpath exists
    cur.execute('SELECT id FROM filegroups WHERE originpath = ?', (originpath,))
    if cur.fetchone():
        return False
    now = datetime.now(timezone.utc).isoformat()
    cur.execute('INSERT INTO filegroups (originpath, destinationpath, filename, extension, hash, copied, duplicate, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (originpath, None, filename, extension, filehash, 0, None, now))
    conn.commit()
    return True


def update_filegroup_copied(conn, originpath, destinationpath):
    cur = conn.cursor()
    cur.execute('UPDATE filegroups SET copied = 1, destinationpath = ? WHERE originpath = ?', (destinationpath, originpath))
    conn.commit()


def set_filegroup_duplicate(conn, originpath):
    cur = conn.cursor()
    cur.execute('UPDATE filegroups SET duplicate = 1 WHERE originpath = ?', (originpath,))
    conn.commit()

# -------------------------
# hashvalues upsert / lookup helpers
# -------------------------

def get_hash_for_path(conn, path):
    cur = conn.cursor()
    cur.execute('SELECT hash FROM hashvalues WHERE path = ?', (path,))
    r = cur.fetchone()
    return r[0] if r else None


def insert_or_update_hashvalue(conn, path, filehash):
    cur = conn.cursor()
    # try update existing row
    cur.execute('SELECT id FROM hashvalues WHERE path = ?', (path,))
    r = cur.fetchone()
    if r:
        cur.execute('UPDATE hashvalues SET hash = ? WHERE id = ?', (filehash, r[0]))
    else:
        # find a row counter value
        cur.execute('SELECT MAX(row) FROM hashvalues')
        rowmax = cur.fetchone()[0]
        if rowmax is None:
            rownum = 0
        else:
            rownum = int(rowmax) + 1
        cur.execute('INSERT INTO hashvalues (row, path, hash, duplicate, deleted) VALUES (?, ?, ?, ?, ?)',
                    (rownum, path, filehash, None, 0))
    conn.commit()


def mark_hashvalues_duplicate(conn, hash_value):
    cur = conn.cursor()
    # set duplicate=1 for rows with this hash where duplicate != 1
    cur.execute('UPDATE hashvalues SET duplicate = 1 WHERE hash = ? AND (duplicate IS NULL OR duplicate = 0)', (hash_value,))
    conn.commit()








# -------------------------
# generate_sql generator
# -------------------------

def generate_sql(start_dir, db_path, resume=False, cancel_event=None, dry_run=False):
    """
    Walks start_dir, computes MD5 hashes, writes to sqlite database at db_path.
    Supports resume via metadata keys and cooperative cancel via cancel_event.
    Yields events: ("status", str), ("log", str), ("progress", float), ("file", str), ("done", None), ("error", str)
    """
    # verify directory
    if not os.path.isdir(start_dir):
        yield ("error", f"Directory does not exist: {start_dir}")
        yield ("status", "error")
        return

    yield ("status", "Scanning filesystem")
    yield ("log", f"Starting directory scan: {start_dir}")

    # first pass: build list of files to process (respecting .git/GIT exclusion)
    file_list = []
    for root, dirs, files in os.walk(start_dir):
        dirs.sort()
        files.sort()
        dirs[:] = [d for d in dirs if d.upper() != "GIT" and d != ".git"]
        for f in files:
            file_list.append(os.path.join(root, f))

    total_files = len(file_list)
    if total_files == 0:
        yield ("error", "Directory contains no files.")
        yield ("status", "error")
        return

    yield ("log", f"Total files found: {total_files}")
    yield ("status", "Preparing database")

    # connect to DB
    try:
        conn = sqlite3.connect(db_path)
        _ensure_metadata_table(conn)
        cur = conn.cursor()
        # ensure hashvalues table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS hashvalues
            (row INT, path TEXT, hash TEXT, duplicate INT, deleted INT)
        """)
        # index on hash
        cur.execute("CREATE INDEX IF NOT EXISTS idx_hash ON hashvalues(hash)")
        conn.commit()
    except Exception as e:
        yield ("error", f"Database error: {e}")
        yield ("status", "error")
        return

    # load existing paths set and metadata
    try:
        cur.execute("SELECT path FROM hashvalues")
        existing_paths = set(r[0] for r in cur.fetchall())
    except Exception:
        existing_paths = set()

    # metadata handling
    if not resume:
        set_metadata(conn, 'operation_state', 'generate_sql')
        set_metadata(conn, 'last_operation', 'generate_sql')
        set_metadata(conn, 'last_file', '')
        set_metadata(conn, 'processed_count', str(0))
        set_metadata(conn, 'total_files', str(total_files))
        processed = 0
    else:
        # resume expects the metadata 'processed_count' to reflect how many files were already processed
        processed = int(get_metadata(conn, 'processed_count', '0'))
        # protect against broken metadata
        if processed < 0:
            processed = 0
        if processed > total_files:
            processed = total_files
        # ensure metadata total_files is updated to new total
        set_metadata(conn, 'total_files', str(total_files))
        set_metadata(conn, 'operation_state', 'generate_sql')
        set_metadata(conn, 'last_operation', 'generate_sql')

    yield ("log", f"Resuming: {len(existing_paths)} files already recorded in DB" if resume else f"Starting fresh generation")

    row_counter = int(get_metadata(conn, 'row_counter', str(0))) if get_metadata(conn, 'row_counter') else 0

    # iterate through files in stable order
    for filepath in file_list:
        # cooperative cancel
        if cancel_event and cancel_event.is_set():
            set_metadata(conn, 'operation_state', 'cancelled')
            set_metadata(conn, 'last_operation', 'generate_sql')
            set_metadata(conn, 'last_file', filepath)
            set_metadata(conn, 'processed_count', str(processed))
            conn.commit()
            yield ("log", "Operation cancelled by user")
            yield ("status", "cancelled")
            conn.close()
            return

        # progress update
        processed += 1
        percent = (processed / total_files) * 100.0
        if percent > 100.0:
            percent = 100.0
        yield ("progress", percent)
        yield ("file", filepath)

        # skip if already in DB
        if filepath in existing_paths:
            # update metadata so resume knows where we are
            set_metadata(conn, 'last_file', filepath)
            set_metadata(conn, 'processed_count', str(processed))
            continue

        # hash file

### V replace with utility helper?
###        #### filehash = compute_md5(filepath, blocksize=65536)
        try:
            md5 = hashlib.md5()
            with open(filepath, 'rb') as fh:
                for chunk in iter(lambda: fh.read(8192), b''):
                    md5.update(chunk)
            filehash = md5.hexdigest()
        except Exception as e:
            yield ("log", f"[ERROR] Could not hash {filepath}: {e}")
            set_metadata(conn, 'last_file', filepath)
            set_metadata(conn, 'processed_count', str(processed))
            continue
##  ^ replace with utility helper?

        # insert row
        try:
            cur.execute(
                'INSERT INTO hashvalues (row, path, hash, duplicate, deleted) VALUES (?, ?, ?, ?, ?)',
                (row_counter, filepath, filehash, None, 0)
            )
            row_counter += 1
            existing_paths.add(filepath)
        except Exception as e:
            yield ("log", f"[DB ERROR] Failed to insert {filepath}: {e}")

        # periodic commit
        if row_counter % 100 == 0:
            conn.commit()

        # update metadata
        set_metadata(conn, 'row_counter', str(row_counter))
        set_metadata(conn, 'last_file', filepath)
        set_metadata(conn, 'processed_count', str(processed))

    # finalize
    set_metadata(conn, 'operation_state', 'idle')
    set_metadata(conn, 'last_operation', 'generate_sql')
    set_metadata(conn, 'last_file', '')
    set_metadata(conn, 'processed_count', str(processed))
    set_metadata(conn, 'total_files', str(total_files))
    conn.commit()
    conn.close()

    yield ("status", "done")
    yield ("log", "Database generation complete.")
    yield ("done", None)


# -------------------------
# generate_and_mark_duplicates
# -------------------------

def generate_and_mark_duplicates(db_path, resume=False, cancel_event=None, dry_run=False):
    """
    Identifies duplicate groups (based on hash) and marks duplicate rows with duplicate=1.
    Option B: only groups where duplicate is NULL or 0 are processed — incremental.
    Yields: ("status","log","progress","group_start","group_dup","duplicate_count","done","error")
    """
    if not os.path.exists(db_path):
        yield ("error", f"Database not found: {db_path}")
        yield ("status", "error")
        return

    yield ("status", "Preparing duplicate marking")
    yield ("log", f"Opening DB: {db_path}")

    conn = sqlite3.connect(db_path)
    _ensure_metadata_table(conn)
    cur = conn.cursor()

    # check for prior duplicates
    cur.execute("SELECT COUNT(*) FROM hashvalues WHERE duplicate = 1")
    baselineduplicates = cur.fetchone()[0]
    yield ("duplicate_count", baselineduplicates)

    # ensure index
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hash ON hashvalues(hash)")
    conn.commit()

    # select hashes that still need processing (groups with more than one row and not fully marked)
    cur.execute("""
        SELECT hash
        FROM hashvalues
        GROUP BY hash
        HAVING COUNT(*) > 1
           AND SUM(CASE WHEN duplicate = 1 THEN 1 ELSE 0 END) < COUNT(*)
    """)
    rows = cur.fetchall()
    hash_rows = [r[0] for r in rows]
    total_groups = len(hash_rows)

    if total_groups == 0:
        yield ("log", "No duplicate groups to process.")
        yield ("status", "done")
        yield ("duplicate_count", 0)
        yield ("done", None)
        conn.close()
        return

    # resume metadata
    if resume:
        last_hash = get_metadata(conn, 'dup_last_hash', '')
        processed_groups = int(get_metadata(conn, 'dup_processed_count', '0'))
        # Clamp
        if processed_groups < 0:
            processed_groups = 0
        if processed_groups > total_groups:
            processed_groups = total_groups
        yield ("log", f"Resuming duplicate marking at hash: {last_hash}" if last_hash else "Resuming duplicate marking")
    else:
        last_hash = ''
        processed_groups = 0
        set_metadata(conn, 'operation_state', 'calculating_duplicates')
        set_metadata(conn, 'last_operation', 'calculate_duplicates')
        set_metadata(conn, 'dup_last_hash', '')
        set_metadata(conn, 'dup_processed_count', '0')
        set_metadata(conn, 'dup_total_groups', str(total_groups))

    duplicate_count = baselineduplicates + 0
    yield ("status", "Scanning duplicate groups")

    # control resume skipping
    start_skipping = bool(resume and last_hash != '')

    for hash_value in hash_rows:
        # cancel check
        if cancel_event and cancel_event.is_set():
            set_metadata(conn, 'operation_state', 'cancelled')
            set_metadata(conn, 'last_operation', 'calculate_duplicates')
            set_metadata(conn, 'dup_last_hash', hash_value)
            set_metadata(conn, 'dup_processed_count', str(processed_groups))
            conn.commit()
            yield ("log", "Duplicate marking cancelled by user")
            yield ("status", "cancelled")
            conn.close()
            return

        # resume skip logic
        if start_skipping:
            if hash_value == last_hash:
                start_skipping = False
            processed_groups += 1
            continue

        processed_groups += 1
        # compute percent relative to groups that will be processed
        percent = (processed_groups / total_groups) * 100.0
        if percent > 100.0:
            percent = 100.0
        yield ("progress", percent)

        # fetch rows for this hash ordered by insertion (_rowid_)
        cur.execute("SELECT _rowid_, path, duplicate FROM hashvalues WHERE hash = ? ORDER BY row ASC, _rowid_ ASC", (hash_value,))
        group_rows = cur.fetchall()

#         filter excluded paths (same rule as generate_sql)
        visible = [(rid, p, dup) for (rid, p, dup) in group_rows if '/GIT/' not in p.upper() and '.git' not in p]

        if not visible:
            # nothing visible to process for this hash
            set_metadata(conn, 'dup_last_hash', hash_value)
            set_metadata(conn, 'dup_processed_count', str(processed_groups))
            continue

        base_rowid, base_path, base_dup = visible[0]
        yield ("group_start", base_path)

        # collect rowids to mark as duplicate (skip those already marked)
        dup_rowids = []
        for (rid, p, dup) in visible[1:]:
 #           if dup != 1:
            if dup is None or dup == 0:
                dup_rowids.append(rid)
                duplicate_count += 1
                yield ("group_dup", p)
                yield ("duplicate_count", duplicate_count)

        if dup_rowids:
            # perform single UPDATE for group
            placeholders = ','.join('?' for _ in dup_rowids)
            sql = f"UPDATE hashvalues SET duplicate = 1 WHERE _rowid_ IN ({placeholders})"
            cur.execute(sql, dup_rowids)

        # persist metadata at group boundary
        set_metadata(conn, 'dup_last_hash', hash_value)
        set_metadata(conn, 'dup_processed_count', str(processed_groups))
        conn.commit()

    # finalize
    set_metadata(conn, 'operation_state', 'idle')
    set_metadata(conn, 'last_operation', 'calculate_duplicates')
    set_metadata(conn, 'dup_last_hash', '')
    set_metadata(conn, 'dup_processed_count', str(processed_groups))
    set_metadata(conn, 'dup_total_groups', str(total_groups))
    conn.commit()
    conn.close()

    yield ("status", "done")
    yield ("duplicate_count", duplicate_count)
    yield ("log", f"Duplicate marking complete. Total duplicates: {duplicate_count}")
    yield ("done", None)

# -------------------------
# delete_duplicates function
# -------------------------

def delete_duplicates(self):
    """Ask user to confirm, then run delete duplicates generator."""
    answer = messagebox.askyesno(
        "Confirm Delete",
        "Are you SURE you want to delete ALL duplicate files?\n"
        "This action CANNOT be undone."
    )
    if not answer:
        self.status_data_label.config(text="Delete cancelled by user.")
        return

    # If confirmed → run normally
    self.start_long_task(
        generator_func=delete_duplicates_generator,
        startdir=self.entry_dir.get(),
        dbpath=self.entry_dbname.get()
    )

# -------------------------
# delete_duplicates generator
# -------------------------

def delete_duplicates_generator(db_path, resume=False, cancel_event=None, dry_run=False):
    """
    Deletes duplicates as determined by duplicate=1 and deleted=0 rows. Keeps the first file in each hash group.
    If dry_run=True the generator only reports what would be deleted without deleting.
    Resume semantics supported via del_last_hash / del_processed_count.
    Yields: ("status","log","progress","deleted", "done","error")
    """
    import os

    if not os.path.exists(db_path):
        yield ("error", f"Database not found: {db_path}")
        yield ("status", "error")
        return

    yield ("status", "Preparing delete operation")
    yield ("log", f"Opening DB: {db_path}")

    conn = sqlite3.connect(db_path)
    _ensure_metadata_table(conn)
    cur = conn.cursor()
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hash ON hashvalues(hash)")
    conn.commit()

    # build list of hashes where duplicate=1 exists and deleted=0 for the duplicates
    cur.execute("""
        SELECT hash
        FROM hashvalues
        GROUP BY hash
        HAVING SUM(CASE WHEN duplicate = 1 AND deleted = 0 THEN 1 ELSE 0 END) > 0
    """)
    rows = cur.fetchall()
    hash_rows = [r[0] for r in rows]
    total_groups = len(hash_rows)

    if total_groups == 0:
        yield ("log", "No duplicate groups pending deletion.")
        yield ("status", "done")
        yield ("done", None)
        conn.close()
        return

    # resume state
    if resume:
        last_hash = get_metadata(conn, 'del_last_hash', '')
        processed_groups = int(get_metadata(conn, 'del_processed_count', '0'))
        if processed_groups < 0:
            processed_groups = 0
        if processed_groups > total_groups:
            processed_groups = total_groups
        yield ("log", f"Resuming delete at hash: {last_hash}" if last_hash else "Resuming delete")
    else:
        last_hash = ''
        processed_groups = 0
        set_metadata(conn, 'operation_state', 'delete_duplicates')
        set_metadata(conn, 'last_operation', 'delete_duplicates')
        set_metadata(conn, 'del_last_hash', '')
        set_metadata(conn, 'del_processed_count', '0')
        set_metadata(conn, 'del_total_groups', str(total_groups))

    yield ("status", "Deleting duplicates")
    start_skipping = bool(resume and last_hash != '')
    deleted_count = 0

    for hash_value in hash_rows:
        # cancel
        if cancel_event and cancel_event.is_set():
            set_metadata(conn, 'operation_state', 'cancelled')
            set_metadata(conn, 'last_operation', 'delete_duplicates')
            set_metadata(conn, 'del_last_hash', hash_value)
            set_metadata(conn, 'del_processed_count', str(processed_groups))
            conn.commit()
            yield ("log", "Delete operation cancelled by user")
            yield ("status", "cancelled")
            conn.close()
            return

        if start_skipping:
            if hash_value == last_hash:
                start_skipping = False
            processed_groups += 1
            continue

        processed_groups += 1
        percent = (processed_groups / total_groups) * 100.0
        if percent > 100.0:
            percent = 100.0
        yield ("progress", percent)

        # fetch rows ordered by rowid
        cur.execute("SELECT _rowid_, path, duplicate, deleted FROM hashvalues WHERE hash = ? ORDER BY _rowid_ ASC" , (hash_value,))
        group_rows = cur.fetchall()
        visible = [(rid, p, dup, delflag) for (rid, p, dup, delflag) in group_rows if '/GIT/' not in p.upper() and '.git' not in p]

        if not visible:
            set_metadata(conn, 'del_last_hash', hash_value)
            set_metadata(conn, 'del_processed_count', str(processed_groups))
            continue

        # keep first visible as base
        base = visible[0]
        base_rowid = base[0]
        # candidates to delete: visible[1:] but only those where duplicate==1 and deleted==0
        to_delete = []
        for (rid, p, dup, dflag) in visible[1:]:
            if dup == 1 and dflag == 0:
                to_delete.append((rid, p))

        for (rid, path) in to_delete:
            # attempt delete
            try:
                if not dry_run:
                    os.remove(path)
                    # update DB deleted flag
                    cur.execute('UPDATE hashvalues SET deleted = 1 WHERE _rowid_ = ?', (rid,))
                    conn.commit()
                deleted_count += 1
                yield ("deleted", path)
#                yield ("log", f"File Deleted: {path}")
            except Exception as e:
                yield ("log", f"Failed to delete {path}: {e}")

        # persist metadata
        set_metadata(conn, 'del_last_hash', hash_value)
        set_metadata(conn, 'del_processed_count', str(processed_groups))
        conn.commit()

    set_metadata(conn, 'operation_state', 'idle')
    set_metadata(conn, 'last_operation', 'delete_duplicates')
    set_metadata(conn, 'del_last_hash', '')
    set_metadata(conn, 'del_processed_count', str(processed_groups))
    set_metadata(conn, 'del_total_groups', str(total_groups))
    conn.commit()
    conn.close()

    yield ("status", "done")
    yield ("log", f"Delete operation complete. Deleted {deleted_count} files.")
    yield ("done", None)


# -------------------------
# generator: group_files_generator
# -------------------------

def group_files_generator(origin_dir, dest_dir, db_path, grouping_mode='ext_with_dot', copy_duplicates=False, resume=False, cancel_event=None, dry_run=False, commit_every=100):
    """
    Generator that groups files by extension/filename and optionally copies them.

    Yields events compatible with the GUI:
      ("status", str)
      ("log", str)
      ("progress", float)
      ("group_entry", originpath, destinationpath_or_None)
      ("copied", path)
      ("skipped_duplicate", path)
      ("done", None)
      ("error", str)

    Resume semantics:
      - If resume=False: scan origin_dir, insert filegroups rows for all files not already present.
      - If resume=True: do NOT rescan; process filegroups rows where copied=0 in id order.
    """
    # basic validation
    if not os.path.isdir(origin_dir):
        yield ("error", f"Origin folder does not exist: {origin_dir}")
        yield ("status", "error")
        return
    if not os.path.isdir(dest_dir):
        # attempt to create
        try:
            os.makedirs(dest_dir, exist_ok=True)
        except Exception as e:
            yield ("error", f"Cannot create destination: {e}")
            yield ("status", "error")
            return

    # open DB and ensure schema
    try:
        conn = sqlite3.connect(db_path)
        ensure_schema_for_grouper(conn)
        cur = conn.cursor()
    except Exception as e:
        yield ("error", f"Database error: {e}")
        yield ("status", "error")
        return

    yield ("status", "Preparing filegroup operation")
    yield ("log", f"Origin: {origin_dir} -> Destination: {dest_dir}")

    # If resume=False, build the file list and insert into filegroups as needed
    if not resume:
        # build deterministic file list
        file_list = []
        for root, dirs, files in os.walk(origin_dir):
            dirs.sort()
            files.sort()
            for f in files:
                file_list.append(os.path.join(root, f))

        total_files = len(file_list)
        yield ("log", f"Files found: {total_files}")

        # Insert into filegroups if not present
        inserted = 0
        for path in file_list:
            if cancel_event and cancel_event.is_set():
                set_metadata(conn, 'fg_operation_state', 'cancelled')
                conn.commit()
                yield ("log", "FileGrouper cancelled during build")
                yield ("status", "cancelled")
                conn.close()
                return

            filename = os.path.basename(path)
#            "Group by Extension", "Group by Extension, (dot) prefix", "Group by filename"
            extension = get_extension_variant(path, mode=('filename' if grouping_mode=='Group by filename' else ('ext' if grouping_mode=='Group by Extension' else 'ext_with_dot')))

            # attempt to reuse existing hash value if present
            stored_hash = get_hash_for_path(conn, path)
            if stored_hash:
                filehash = stored_hash
            else:
                # compute hash lazily (we can defer hashing until copy decision)
                filehash = None

            inserted_flag = insert_filegroup_entry(conn, path, filename, extension, filehash)
            if inserted_flag:
                inserted += 1

        # initial metadata
        set_metadata(conn, 'fg_total_files', str(total_files))
        set_metadata(conn, 'fg_processed_count', '0')
        set_metadata(conn, 'fg_last_path', '')
        set_metadata(conn, 'fg_operation_state', 'ready')
        conn.commit()

    # Workset determination
    # For resume, process entries where copied=0
    cur.execute('SELECT COUNT(*) FROM filegroups WHERE copied = 0')
    to_process = cur.fetchone()[0]
    if to_process == 0:
        yield ("log", "No files to process (copied=0)")
        yield ("status", "done")
        yield ("done", None)
        conn.close()
        return

    # For progress calculation, use this workset
    cur.execute('SELECT id, originpath, filename, extension, hash FROM filegroups WHERE copied = 0 ORDER BY id ASC')
    rows = cur.fetchall()
    total = len(rows)
    processed = int(get_metadata(conn, 'fg_processed_count', '0')) if resume else 0
    if processed < 0:
        processed = 0
    if processed > total:
        processed = total

    yield ("log", f"Processing {total} filegroup entries (resume={resume})")
    set_metadata(conn, 'fg_operation_state', 'running')
    set_metadata(conn, 'fg_last_operation', 'filegroup_copy')
    conn.commit()

    # iterate workset
    new_processed = 0
    commit_counter = 0
    copied_count = 0
    skipped_count = 0

    for (fg_id, originpath, filename, extension, filehash) in rows:
        if cancel_event and cancel_event.is_set():
            set_metadata(conn, 'fg_operation_state', 'cancelled')
            set_metadata(conn, 'fg_last_path', originpath)
            set_metadata(conn, 'fg_processed_count', str(processed + new_processed))
            conn.commit()
            yield ("log", "FileGrouper cancelled by user")
            yield ("status", "cancelled")
            conn.close()
            return

        # compute percent and yield
        new_processed += 1
        processed_total = processed + new_processed
        percent = (processed_total / total) * 100.0 if total > 0 else 100.0
        if percent > 100.0:
            percent = 100.0
        yield ("progress", percent)

        # ensure we have a filehash (try to reuse, otherwise compute)
        if not filehash:
            filehash = get_hash_for_path(conn, originpath)
            if not filehash:
                filehash = compute_md5(originpath)
                if filehash:
                    insert_or_update_hashvalue(conn, originpath, filehash)

        # decide duplicate state using hashvalues
        is_duplicate = False
        if filehash:
            cur.execute('SELECT COUNT(*) FROM hashvalues WHERE hash = ?', (filehash,))
            cnt = cur.fetchone()[0]
            if cnt > 0:
                is_duplicate = True

        # if it's a duplicate and policy says don't copy duplicates
        destpath = None
        if is_duplicate and not copy_duplicates:
            # mark duplicate in both tables
            set_filegroup_duplicate(conn, originpath)
            mark_hashvalues_duplicate(conn, filehash)
            skipped_count += 1
            yield ("skipped_duplicate", originpath)
            # persist metadata
            set_metadata(conn, 'fg_last_path', originpath)
            set_metadata(conn, 'fg_processed_count', str(processed_total))
            conn.commit()
            commit_counter += 1
            if commit_counter >= commit_every:
                commit_counter = 0
            continue

        # otherwise, copy
        # build destination path: dest_dir / extension / filename
        safe_ext = extension.replace(os.sep, '_').replace(':', '_')
        dest_folder = os.path.join(dest_dir, safe_ext)
        destpath = os.path.join(dest_folder, filename)

        if dry_run:
            # simulate
            copied = False
        else:
            copied = safe_copy(originpath, destpath, overwrite=False)

        if copied:
            update_filegroup_copied(conn, originpath, destpath)
            copied_count += 1
            yield ("copied", originpath)
            # also update hashvalues to reflect this file (if not present already)
            if filehash:
                insert_or_update_hashvalue(conn, originpath, filehash)
        else:
            # if copy didn't happen because file exists at destination we still mark as copied to avoid retry loops
            # but do not overwrite by default
            # mark as copied if destination exists
            if os.path.exists(destpath):
                update_filegroup_copied(conn, originpath, destpath)
                copied_count += 1
                yield ("copied", originpath)
            else:
                # copy failed
                yield ("log", f"Failed to copy: {originpath} -> {destpath}")

        # persist metadata after each iteration
        set_metadata(conn, 'fg_last_path', originpath)
        set_metadata(conn, 'fg_processed_count', str(processed_total))
        conn.commit()
        commit_counter += 1

    # finalize
    set_metadata(conn, 'fg_operation_state', 'idle')
    set_metadata(conn, 'fg_last_operation', 'filegroup_copy')
    set_metadata(conn, 'fg_last_path', '')
    set_metadata(conn, 'fg_processed_count', str(processed + new_processed))
    set_metadata(conn, 'fg_total_files', str(total))
    conn.commit()
    conn.close()

    yield ("status", "done")
    yield ("log", f"FileGrouper complete. Copied: {copied_count}, Skipped: {skipped_count}")
    yield ("done", None)


# -------------------------
# generator: delete empty folders
# -------------------------


def delete_empty_folders_generator(startdir, *, resume=False, cancel_event=None, dry_run=False):
    """
    Generator that walks a directory tree and deletes empty folders (bottom-up).
    Emits events compatible with the existing event handler system.
    """

    import os
    from pathlib import Path

    if cancel_event is None:
        class Dummy:
            def is_set(self): return False
        cancel_event = Dummy()

    start_path = Path(startdir).resolve()

    if not start_path.exists() or not start_path.is_dir():
        yield ("error", f"Directory does not exist: {start_path}")
        return

    yield ("status", f"Scanning for empty folders under: {start_path}")

    # -------------------------------
    # Phase 1: Count all directories
    # -------------------------------
    try:
        total_dirs = 0
        for _, dirs, _ in os.walk(start_path):
            total_dirs += len(dirs)
    except Exception as e:
        yield ("error", f"Error during directory counting: {e}")
        return

    if total_dirs == 0:
        yield ("status", "No subdirectories found.")
        yield ("done", None)
        return

    yield ("log", f"Found {total_dirs} directories to evaluate.")
    yield ("status", "Evaluating directories...")

    # Progress counters
    processed = 0
    deleted_count = 0

    # ----------------------------------------
    # Phase 2: Evaluate & delete (bottom-up)
    # ----------------------------------------
    for root, dirs, files in os.walk(start_path, topdown=False):

        if cancel_event.is_set():
            yield ("status", "Cancelled by user.")
            yield ("done", None)
            return

        for d in dirs:
            folder_path = Path(root) / d
            processed += 1

            # Check emptiness
            try:
                if not any(folder_path.iterdir()):
                    if dry_run:
                        yield ("log", f"[DRY RUN] Would delete: {folder_path}")
                        deleted_count += 1
                    else:
                        try:
                            folder_path.rmdir()
                            deleted_count += 1
                            yield ("log", f"Deleted empty folder: {folder_path}")
                        except Exception as e:
                            yield ("error", f"Failed to delete {folder_path}: {e}")
                else:
                    yield ("log", f"Not empty: {folder_path}")
            except Exception as e:
                yield ("error", f"Error accessing {folder_path}: {e}")

            # Progress update
            pct = (processed / total_dirs) * 100
            yield ("progress", pct)

    yield ("status", f"Deletion complete. Removed {deleted_count} empty folders.")
    yield ("done", None)




########################################################### GUI

# -------------------------
# Tkinter App
# -------------------------
class App:
    def __init__(self, root):
        self.root = root
        self.root.title("File Utility Tool GUI")
        self.root.geometry("700x500")

        # Log lines limit
        self.MAX_LOG_LINES = 5000

        # cancellation event
        self.cancel_event = threading.Event()
        self.task_active = False

        notebook = ttk.Notebook(self.root)
        notebook.pack(fill="both", expand=True)

        # Input tab
        filecomp_tab = ttk.Frame(notebook)
        notebook.add(filecomp_tab, text="FILECOMP")

        ##Filecomp GUI Structure - FRAMES
        filecomp_source = ttk.Frame(filecomp_tab)
        filecomp_source.pack(pady=5)
        db_source = ttk.Frame(filecomp_tab)
        db_source.pack(pady=5)
        status_data = ttk.Frame(filecomp_tab)
        status_data.pack(pady=5)
        filecomp_buttons = ttk.Frame(filecomp_tab)
        filecomp_buttons.pack(pady=5)
        progress_frame = ttk.Frame(filecomp_tab)
        progress_frame.pack(pady=5)
        cancel_resume_buttons = ttk.Frame(filecomp_tab)
        cancel_resume_buttons.pack(pady=5)

        # Directory entry
        ttk.Label(filecomp_source, text="Start Directory:").pack(pady=5)
        self.entry_dir = ttk.Entry(filecomp_source, width=40)   
        self.entry_dir.pack(side=tk.LEFT,pady=5)
        ttk.Button(filecomp_source, text="Select Folder", command=lambda:self.select_directory(self.entry_dir)).pack(side=tk.LEFT, padx=5)

        # DB entry
        ttk.Label(db_source, text="Database File:").pack(pady=5)
        self.entry_dbname = ttk.Entry(db_source, width=40)
        self.entry_dbname.insert(0, defaultdbname)
        self.entry_dbname.pack(side=tk.LEFT,pady=5)
        ttk.Button(db_source, text="Select Database File", command=self.select_db_file).pack(side=tk.LEFT,padx=5)


        # Buttons
        input_frame_buttons = ttk.Frame(filecomp_buttons)
        input_frame_buttons.pack(pady=10)

        ttk.Button(input_frame_buttons, text="Generate SQL", command=self.start_generate_sql).pack(side=tk.LEFT, padx=5)
        ttk.Button(input_frame_buttons, text="Calculate Duplicates", command=self.calculate_duplicates).pack(side=tk.LEFT, padx=5)
        ttk.Button(input_frame_buttons, text="Delete Duplicates", command=self.start_delete_duplicates).pack(side=tk.LEFT, padx=5)

        # Status
        self.status_data_label = ttk.Label(status_data, text="Status: Idle")
        self.status_data_label.pack(side=tk.LEFT, pady=5, padx=15)

        # Duplicates label
        self.duplicates_label = ttk.Label(status_data, text="Duplicates: 0")
        self.duplicates_label.pack(side=tk.LEFT, pady=5, padx=15)

        # Progress
        self.progress_label = ttk.Label(progress_frame, text="Progress: 0%")
        self.progress_label.pack(pady=5)
        self.progress = ttk.Progressbar(progress_frame, orient="horizontal", length=600, mode="determinate")
        self.progress.pack(pady=5)

        # Cancel and Resume Buttons
        ttk.Button(cancel_resume_buttons, text="Cancel", command=self.cancel_action).pack(side=tk.LEFT, padx=5)
        ttk.Button(cancel_resume_buttons, text="Resume", command=self.resume_action).pack(side=tk.LEFT, padx=5)
        ttk.Button(cancel_resume_buttons, text='Delete Default DB', command=self.delete_default_db).pack(side=tk.LEFT,padx=5)

##
##        ########### File Grouper Tab
##
        filegroup_tab = ttk.Frame(notebook)
        notebook.add(filegroup_tab, text="FileGrouper")

        filegroup_source= ttk.Frame(filegroup_tab)
        filegroup_source.pack(pady=5)
        filegroup_destination = ttk.Frame(filegroup_tab)
        filegroup_destination.pack(pady=5)
        db_source2 = ttk.Frame(filegroup_tab)
        db_source2.pack(pady=5)       
        filegroup_selectors = ttk.Frame(filegroup_tab)
        filegroup_selectors.pack(pady=5)
        filegroup_status_data = ttk.Frame(filegroup_tab)
        filegroup_selectors.pack(pady=5)
        status_data2 = ttk.Frame(filegroup_tab)
        status_data2.pack(pady=5)
        filegroup_buttons = ttk.Frame(filegroup_tab)
        filegroup_buttons.pack(pady=5)
        progress_frame2 = ttk.Frame(filegroup_tab)
        progress_frame2.pack(pady=5)
        buttons2 = ttk.Frame(filegroup_tab)
        buttons2.pack(pady=5)

        ttk.Label(filegroup_source, text="Origin Directory:").pack(pady=5)
        self.filegroupstart_dir = ttk.Entry(filegroup_source, width=40)
        self.filegroupstart_dir.pack(side=tk.LEFT,pady=2)
        ttk.Button(filegroup_source, text="Select Folder", command=lambda:self.select_directory(self.filegroupstart_dir)).pack(side=tk.LEFT, padx=5)

        ttk.Label(filegroup_destination, text="Destination Directory:").pack(pady=2)
        self.filegroupdest_dir = ttk.Entry(filegroup_destination, width=40)
        self.filegroupdest_dir.pack(side=tk.LEFT,pady=5)
        ttk.Button(filegroup_destination, text="Select Folder", command=lambda:self.select_directory(self.filegroupdest_dir)).pack(side=tk.LEFT, padx=5)

        # DB entry
        ttk.Label(db_source2, text="Database File:").pack(pady=5)
        self.entry_dbname2 = ttk.Entry(db_source2, width=40)
        self.entry_dbname2.pack(side=tk.LEFT,pady=2)
        self.entry_dbname2.insert(0, defaultdbname)
        self.entry_dbname2.pack(side=tk.LEFT,pady=5)
        ttk.Button(db_source2, text="Select Database File", command=self.select_db_file).pack(side=tk.LEFT,padx=5)


        # File Action Behavior Selector

        self.filegroupactionselector_label = ttk.Label(filegroup_selectors, text="Grouping Behavior")
        self.filegroupactionselector_label.pack(padx=5)
        self.filegroupactionselector = ttk.Combobox(filegroup_selectors, values=["Group by Extension", "Group by Extension, (dot) prefix", "Group by filename"], width=27)
        self.filegroupactionselector.pack(side=tk.LEFT, pady=5, padx=15)
        self.filegroupactionselector.insert(0, "Group by Extension")

        filegroupradiobuttons_frame = tk.Frame(filegroup_selectors)
        self.copyduplicatesselector = tk.StringVar(value="nocopyduplicates")
        ttk.Radiobutton(filegroupradiobuttons_frame, text= "Do Not Copy Duplicates", variable=self.copyduplicatesselector, value="nocopyduplicates").pack(padx=5, pady=5)
        ttk.Radiobutton(filegroupradiobuttons_frame, text= "Copy Duplicates", variable=self.copyduplicatesselector, value='copyduplicates').pack(side=tk.BOTTOM, padx=5, pady=5)
        filegroupradiobuttons_frame.pack(padx=10)

        # Status Labels

        self.status_data_label2 = ttk.Label(status_data2, text="Status: Idle")
        self.status_data_label2.pack(side=tk.LEFT, pady=5, padx=5)


        # Progress
        self.progress_label2 = ttk.Label(progress_frame2, text="Progress: 0%")
        self.progress_label2.pack(pady=5)
        self.progress2 = ttk.Progressbar(progress_frame2, orient="horizontal", length=600, mode="determinate")
        self.progress2.pack(pady=5)

        # File Copy Run, Cancel and Resume Buttons
        ttk.Button(buttons2, text="Run", command=self.run_filegroup).pack(side=tk.LEFT, padx=5)
        ttk.Button(buttons2, text="Cancel", command=self.cancel_action).pack(side=tk.LEFT, padx=5)
        ttk.Button(buttons2, text="Resume", command=self.resume_action).pack(side=tk.LEFT, padx=5)
        ttk.Button(buttons2, text='Delete Default DB', command=self.delete_default_db).pack(side=tk.LEFT,padx=5)


        ############ Directory Cleaner

        # DirectoryCleaner tab
        directorycleaner_frame = ttk.Frame(notebook)
        notebook.add(directorycleaner_frame, text="DirectoryCleaner")

        deletedir_source= ttk.Frame(directorycleaner_frame)
        deletedir_source.pack(pady=5)
        deletedir_status= ttk.Frame(directorycleaner_frame)
        deletedir_status.pack(pady=5)
#        deletedir_progressstatus= ttk.frame(directorycleaner_frame)
#        deletedir_progressstatus.pack(pady=5)
        deletedir_progressbar=ttk.Frame(directorycleaner_frame)
        deletedir_progressbar.pack(pady=5)
        deletedir_buttons=ttk.Frame(directorycleaner_frame)
        deletedir_buttons.pack(pady=5)


        ttk.Label(deletedir_source, text="Root Directory to Clean:").pack(pady=5)
        self.deletestart_dir = ttk.Entry(deletedir_source, width=40)
        self.deletestart_dir.pack(side=tk.LEFT,pady=2)
        ttk.Button(deletedir_source, text="Select Folder", command=lambda:self.select_directory(self.deletestart_dir)).pack(side=tk.LEFT, padx=5)


        # Status
        self.status_data_label3 = ttk.Label(deletedir_status, text="Status: Idle")
        self.status_data_label3.pack(side=tk.LEFT, pady=5, padx=5)

        # Progress
        self.progress_label3 = ttk.Label(deletedir_progressbar, text="Progress: 0%")
        self.progress_label3.pack(pady=5)
        self.progress3 = ttk.Progressbar(deletedir_progressbar, orient="horizontal", length=600, mode="determinate")
        self.progress3.pack(pady=5)

        # File Copy Run, Cancel and Resume Buttons
        ttk.Button(deletedir_buttons, text="Run", command=self.run_directorycleaner).pack(side=tk.LEFT, padx=5)
        ttk.Button(deletedir_buttons, text="Cancel", command=self.cancel_action).pack(side=tk.LEFT, padx=5)


        ############ Output Tab

        # Output tab
        output_frame = ttk.Frame(notebook)
        notebook.add(output_frame, text="Output")
        self.output_box = scrolledtext.ScrolledText(output_frame, width=120, height=40)
        self.output_box.pack(fill="both", expand=True)

        ########## About TAB

        about_frame = ttk.Frame(notebook)
        notebook.add(about_frame, text="About")
        self.about_box = scrolledtext.ScrolledText(about_frame, wrap=tk.WORD)
        self.about_box.insert(tk.END,'''File Utility Tool GUI - Ian Hill, 2025

This program is FREEWARE provided for NON COMMERCIAL use and is provided
WITH NO WARRANTY. This program deletes files, use at your own risk.

This program is comprised of a series of utility scripts for file management.

FILECOMP is a utility for detecting and deleting duplicate files:
    Inputs:
        Starting Directory: Where you want the scan to start
        Database File: The database file you want to use, resume, or create.
            (NOTE: a new database should be generated after each mass delete)
    Options:
        Generate SQL: Scans the specified directory and all subdirectories,
            generating an md5 hash of all files
        Calculate Duplicates: Determine the number of duplicate files based on
            md5 hashes in the database, returns the number of duplicate files
        Delete Duplicates: Delete all duplicate files on the filesystem
            as determined by Calculate Duplicates (requires sufficient
            permissions). Retains the FIRST found copy of a file, deleting all
            other identical (not identically named) files.
    WorkFlow:
        Select/Define starting directory, Set database name/use default,
        Create Database, Calculate Duplicates, Run 'Delete Duplicates'
        if desired. Repeat with new database as needed. If one wishes
        one can scan multiple directories into one database and delete
        duplicate files from all locations as built in one database. 

FILEGROUPER is a utility for copying files to directories based on
        file extension or file name
    Inputs:
        Origin Directory: The location where files to be grouped reside
        Destination Directory: where the files will be moved.
            (NOTE: must not be within the origin directory)
        Database File: Database for storing record of action
    Options:
        Grouping Behavior:
            Dropdown:
             Group by Extension: create directories based on extension
             and copy files to the appropriate file extension
             Group with (dot): adds "dot " to extension file, as an
             original request for this functionality
             Group by filename: create directories by filename, ideally only
             for sorting duplicate names, different files
            Do Not Copy Duplicates or Copy Duplicates:
             Do or Do not make copies of identical files based on their md5   
        Workflow:
            Select/Define source directory, Select Define Destination directory,
            Select/Define database file, chose grouping behavior, run process.
DirectoryCleaner is a utility for deleting empty directories recursively
    (NOTE: Does not use database to record actions or resume)
    Inputs:
        Select directory: The starting directory
    Workflow:
        Select directory and run.


The Output Window displays the operating results of the utilities.
            ''')
        self.about_box.config(state=tk.DISABLED)
        self.about_box.pack(fill="both", expand=True)





############################
####    End Gui
####    Helpers and GUI functions
####


    def select_directory(self,target_entry):
        """Open dialog to select the start directory for scanning."""
        dir_path = filedialog.askdirectory(
            title="Select directory"
        )
        if dir_path:
            target_entry.delete(0, tk.END)
            target_entry.insert(0, dir_path)
            self._append_output(f"Directory selected: {dir_path}\n")

    def select_db_file(self):
        """Open dialog to select an existing SQLite database file."""
        file_path = filedialog.askopenfilename(
            title="Select database file",
            filetypes=[("SQLite DB", "*.db *.sqlite *.sqlite3"), ("All files", "*.*")]
        )
        if file_path:
            self.entry_dbname.delete(0, tk.END)
            self.entry_dbname.insert(0, file_path)
            self._append_output(f"Database selected: {file_path}\n")


    # helper to append, with log limit
    def _append_output(self, text):
        try:
            box = self.output_box

            box.insert(tk.END, text)

            # ---- LINE LIMIT ENFORCEMENT ----
            line_count = int(box.index("end-1c").split(".")[0])

            if line_count > self.MAX_LOG_LINES:
                excess = line_count - self.MAX_LOG_LINES
                box.delete("1.0", f"{excess + 1}.0")
            box.yview_moveto(1.0)

        except Exception:
            pass




    # update progress
    def update_progress(self, percent):
        try:
            if percent is None:
                percent = 0.0
            if percent < 0:
                percent = 0.0
            if percent > 100.0:
                percent = 100.0
            self.progress['value'] = percent
            self.progress_label.config(text=f"Progress: {percent:.1f}%")
            self.progress2['value'] = percent
            self.progress_label2.config(text=f"Progress: {percent:.1f}%")
            self.progress3['value'] = percent
            self.progress_label3.config(text=f"Progress: {percent:.1f}%")
            self.root.update_idletasks()
        except Exception:
            pass

    # update status
    def update_status(self, statusmsg):
        try:
            self.status_data_label.config(text=f"Status: {statusmsg}")
            self.status_data_label2.config(text=f"Status: {statusmsg}")
            self.status_data_label3.config(text=f"Status: {statusmsg}")                       
        except Exception:
            pass

    # central handle_event: called via root.after or direct
    def handle_event(self, event, data):
        if event == "log":
            self._append_output(data + "\n")
        elif event == "progress":
            try:
                self.update_progress(float(data))
            except Exception:
                pass
        elif event == "status":
            self.update_status(data)
        elif event == "file":
            self._append_output(f"Processing file: {data}\n")
        elif event == "group_start":
            self._append_output(f"\nBASE: {data}\n")
        elif event == "group_dup":
            self._append_output(f"    DUPLICATE: {data}\n")
        elif event == "duplicate_count":
            try:
                self.duplicates_label.config(text=f"Duplicates: {int(data)}")
            except Exception:
                pass
        elif event == "deleted":
            self._append_output(f"DELETED: {data}\n")
        elif event == "error":
            self._append_output(f"ERROR: {data}\n")
            self.update_status("error")
        elif event == "done":
            self.update_status("Finished")
            self.update_progress(100.0)

    def thread_runner(self, generator_func, args=(), resume=False, cancel_event=None, dry_run=False, gen_kwargs=None):
        """
        Universal runner for generators.

        - Prevents multiple concurrent tasks via self.task_active.
        - Calls the generator with kwargs: resume, cancel_event, dry_run (merged with gen_kwargs).
        - Posts events to GUI via self.root.after(...) to call self.handle_event(event, data).
        """

        # Prevent starting a new task if one is already running
        if getattr(self, 'task_active', False):
            # run in GUI thread
            try:
                self.root.after(0, lambda: messagebox.showwarning(
                    "Task already running",
                    "Another operation is currently in progress. Please wait for it to finish or cancel it before starting a new task."
                ))
            except Exception:
                # fallback direct call (shouldn't be needed)
                messagebox.showwarning(
                    "Task already running",
                    "Another operation is currently in progress. Please wait for it to finish or cancel it before starting a new task."
                )
            return

        # Acquire lock
        self.task_active = True

        # Prepare generator kwargs
        if gen_kwargs is None:
            gen_kwargs = {}
        gen_kwargs.setdefault('resume', resume)
        gen_kwargs.setdefault('cancel_event', cancel_event)
        gen_kwargs.setdefault('dry_run', dry_run)

        try:
            gen = generator_func(*args, **gen_kwargs)

            for item in gen:
                # Each yield expected as (event, payload) or similar - adapt if your generator uses a different shape
                try:
                    ev, payload = item
                except Exception:
                    # If generator yields unexpected format, log and continue
                    try:
                        # schedule log in GUI
                        self.root.after(0, lambda itm=item: self.handle_event('log', f'Bad event from generator: {itm}'))
                    except Exception:
                        self.handle_event('log', f'Bad event from generator: {item}')
                    continue

                # schedule GUI update
                try:
                    self.root.after(0, lambda e=ev, d=payload: self.handle_event(e, d))
                except Exception:
                    # fallback: direct call (rare)
                    self.handle_event(ev, payload)

        except Exception as e:
            # Send exception to GUI via handle_event('error', ...)
            try:
                self.root.after(0, lambda: self.handle_event('error', f'Generator exception: {e}'))
            except Exception:
                self.handle_event('error', f'Generator exception: {e}')

        finally:
            # Always release the task lock no matter what (success, cancel, or exception)
            self.task_active = False
            # Optional: reset cancel_event here as well if desired:
            # if cancel_event is not None: cancel_event.clear()




    # -------------------------
    # Button callbacks and actions
    # -------------------------

    def confirm_destructive_action(self,title,message):
        return messagebox.askyesno(title,message)

    def cancel_action(self):
        """Cancel the running background task."""
        try:
            if hasattr(self, 'cancel_event') and self.cancel_event is not None:
                self.cancel_event.set()
        except Exception:
            pass

        # Release the task lock (generators check the cancel_event cooperatively and will exit;
        # ensure we do not leave the app permanently locked if something went wrong)
        self.task_active = False

        # Update GUI state
        try:
            self.handle_event('status', 'Cancelled')
            self.update_progress(0)
        except Exception:
            pass

    def delete_default_db(self):
        if not self.confirm_destructive_action(
            "Confirm Delete Default DB",
            "Are you SURE you want to delete the default database?\n"
            "This action CANNOT be undone."):
            self.status_data_label.config(text="Status: Delete Database Denied by User")
            self.status_data_label2.config(text="Status: Delete Database Denied by User")
            self.status_data_label3.config(text="Status: Delete Database Denied by User")
            return
        self._append_output("Default database deletion requested\n")
        self._append_output("Default database name: "+ str(defaultdbname)+"\n")
        try:
            if os.path.exists(defaultdbname):
                os.remove(defaultdbname)
                self._append_output('Default database deleted successfully\n')
                self.handle_event('status', 'Default Database Deleted')
            else:
                self._append_output('Default database does not exist\n')
                self.handle_event('status', 'Default DB does not exist')              
        except Exception as e:
            self._append_output('Error deleting default database:\n')
            self.handle_event('status', 'Error Deleting default DB')
            self._append_output(''+str(e)+'\n')

    # start generate sql (fresh)
    def start_generate_sql(self):
        startdir = self.entry_dir.get()
        dbname = self.entry_dbname.get()
        dbpath = dbname
        # reset progress display
        self.update_progress(0)
        self.duplicates_label.config(text="Duplicates: 0")
        self._append_output('\n--- STARTING GENERATE SQL (FRESH) ---\n')
        self.handle_event('status', 'Generating SQL Database')
        # ensure cancel_event exists and cleared
        if not hasattr(self, 'cancel_event'):
            self.cancel_event = threading.Event()
        self.cancel_event.clear()
        t = threading.Thread(target=self.thread_runner, args=(generate_sql, (startdir, dbpath)), kwargs={'resume': False, 'cancel_event': self.cancel_event}, daemon=True)
        t.start()

    # calculate duplicates (fresh)
    def calculate_duplicates(self):
        dbpath = self.entry_dbname.get()
        self.update_progress(0)
        self.duplicates_label.config(text="Duplicates: 0")
        self._append_output('\n--- STARTING CALCULATE DUPLICATES (FRESH) ---\n')
        self.handle_event('status', 'Calculating Duplicates')
        if not hasattr(self, 'cancel_event'):
            self.cancel_event = threading.Event()
        self.cancel_event.clear()
        t = threading.Thread(target=self.thread_runner, args=(generate_and_mark_duplicates, (dbpath,)), kwargs={'resume': False, 'cancel_event': self.cancel_event}, daemon=True)
        t.start()

    # resume for duplicate calculation (helper)
    def resume_calculate_duplicates(self):
        dbpath = self.entry_dbname.get()
        self._append_output('\n--- RESUMING CALCULATE DUPLICATES ---\n')
        self.handle_event('status', 'Resuming Duplicate Scan')
        if not hasattr(self, 'cancel_event'):
            self.cancel_event = threading.Event()
        self.cancel_event.clear()
        t = threading.Thread(target=self.thread_runner, args=(generate_and_mark_duplicates, (dbpath,)), kwargs={'resume': True, 'cancel_event': self.cancel_event}, daemon=True)
        t.start()

    # start delete duplicates (fresh)
    def start_delete_duplicates(self, dry_run=False):
        if not self.confirm_destructive_action(
            "Confirm Delete",
            "Are you SURE you want to delete ALL duplicate files?\n"
            "This action CANNOT be undone."):
            self.status_data_label.config(text="Status: Delete Denied by User")
            self.status_data_label2.config(text="Status: Delete Denied by User")
            self.status_data_label3.config(text="Status: Delete Denied by User")
            return
        dbpath = self.entry_dbname.get()
        self.update_progress(0)
        self._append_output('\n--- STARTING DELETE DUPLICATES ---\n')
        self.handle_event('status', 'Deleting Duplicates')
        if not hasattr(self, 'cancel_event'):
            self.cancel_event = threading.Event()
        self.cancel_event.clear()
        t = threading.Thread(target=self.thread_runner, args=(delete_duplicates_generator, (dbpath,)), kwargs={'resume': False, 'cancel_event': self.cancel_event, 'dry_run': False}, daemon=True)
        t.start()

    # resume delete duplicates helper
    def resume_delete_duplicates(self):
        if not self.confirm_destructive_action(
            "Confirm Delete",
            "Are you SURE you want to delete ALL duplicate files?\n"
            "This action CANNOT be undone."):
            self.status_data_label.config(text="Status: Delete Denied by User")
            self.status_data_label2.config(text="Status: Delete Denied by User")
            self.status_data_label3.config(text="Status: Delete Denied by User")
            return
        dbpath = self.entry_dbname.get()
        self._append_output('\n--- RESUMING DELETE DUPLICATES ---\n')
        self.handle_event('status', 'Resuming Delete Operation')
        if not hasattr(self, 'cancel_event'):
            self.cancel_event = threading.Event()
        self.cancel_event.clear()
        t = threading.Thread(target=self.thread_runner, args=(delete_duplicates_generator, (dbpath,)), kwargs={'resume': True, 'cancel_event': self.cancel_event, 'dry_run': False}, daemon=True)
        t.start()

    def run_filegroup(self):
        """Callback for the FileGrouper RUN button."""
        origin_dir = self.filegroupstart_dir.get().strip()
        dest_dir   = self.filegroupdest_dir.get().strip()
        db_path    = self.entry_dbname.get().strip()   # reuse same DB as FILECOMP

        grouping_mode = self.filegroupactionselector.get().strip()
        copy_duplicates = self.copyduplicatesselector.get().strip()

        # Basic validation
        if not origin_dir:
            self.status_data_label.config(text="Error: No origin directory selected.")
            self.status_data_label2.config(text="Error: No origin directory selected.")
            self.status_data_label3.config(text="Error: No origin directory selected.")           
            return

        if not dest_dir:
            self.status_data_label.config(text="Error: No destination directory selected.")
            self.status_data_label2.config(text="Error: No destination directory selected.")
            self.status_data_label3.config(text="Error: No destination directory selected.")
            return

        if not db_path:
            self.status_data_label.config(text="Error: No database selected.")
            self.status_data_label2.config(text="Error: No database selected.")
            self.status_data_label3.config(text="Error: No database selected.")
            return

        # Convert duplicate policy to boolean
        # Expecting values like "Copy Duplicates" or "Do Not Copy Duplicates"
        copy_dups = ("copy" in copy_duplicates.lower())

        # Reset status & progress UI
        self.progress_label.config(text="Progress: 0%")
        self.progress['value'] = 0
        self.progress_label2.config(text="Progress: 0%")
        self.progress2['value'] = 0
        self.progress_label3.config(text="Progress: 0%")
        self.progress3['value'] = 0        

        try:
            if (dest_dir == origin_dir) or dest_dir.is_relative_to(origin_dir):
                messagebox.showerror(
                    "Invalid Destination",
                    "The Destination folder cannot be inside the source directory.\n"
                    "This would caust infinite recursion while copying")
                return
        except AttributeError:
            #Python < 3.9 fallback
            if str(dest_dir).startswith(str(origin_dir)+os.sep):
                messagebox.showerror(
                "Invalid Destination",
                "The Destination folder cannot be inside the source directory.\n"
                "This would caust infinite recursion while copying")
                return


        self.status_data_label.config(text="FileGrouper: Starting...")
        self.status_data_label2.config(text="FileGrouper: Starting...")
        self.status_data_label3.config(text="FileGrouper: Starting...")
        self.output_box.insert(tk.END, f"[FileGrouper] Begin grouping from: {origin_dir}\n")
        self.output_box.see(tk.END)

        # Fire up thread using your universal runner
        self.cancel_event.clear()

        t = threading.Thread(
            target=self.thread_runner,
            args=(group_files_generator, (origin_dir, dest_dir, db_path, grouping_mode, copy_dups)),
            kwargs={'resume': False, 'cancel_event': self.cancel_event, 'dry_run': False},
            daemon=True
        )
        t.start()

    def run_directorycleaner(self):
        if not self.confirm_destructive_action(
            "Confirm Delete Empty Directories",
            "Are you SURE you want to delete ALL empty directories under this folder?\n"
            "This action CANNOT be undone."):
            self.status_data_label.config(text="Status: DirectoryDelete Denied by User")
            self.status_data_label2.config(text="Status:DirectoryDelete Denied by User")
            self.status_data_label3.config(text="Status: DirectoryDelete Denied by User")
            return
        """Callback to run the Delete Empty Folders generator."""
        startdir = self.deletestart_dir.get().strip()

        if not startdir:
            self.status_data_label.config(text="Error: No directory selected.")
            self.status_data_label2.config(text="Error: No directory selected.")
            self.status_data_label3.config(text="Error: No directory selected.")
            return

        # Reset UI progress indicators
        self.progress_label.config(text="Progress: 0%")
        self.progress['value'] = 0

        # Status + output
        self.status_data_label.config(text="DirectoryCleaner: Starting...")
        self.status_data_label2.config(text="DirectoryCleaner: Starting...")
        self.status_data_label3.config(text="DirectoryCleaner: Starting...")
        self.output_box.insert(tk.END, f"[DirectoryCleaner] Starting cleanup: {startdir}\n")
        self.output_box.see(tk.END)

        # Clear cancellation event if user cancelled a previous run
        self.cancel_event.clear()

        # Start threaded generator
        thread = threading.Thread(
            target=self.thread_runner,
            args=(delete_empty_folders_generator, (startdir,)),
            kwargs={'resume': False, 'cancel_event': self.cancel_event, 'dry_run': False},
            daemon=True
        )
        thread.start()

    

    # -------------------------
    # Resume action (single button detects last operation and resumes appropriately)
    # -------------------------
    def resume_action(self):
        dbpath = self.entry_dbname.get()
        if not os.path.exists(dbpath):
            self._append_output(f"Cannot resume — database not found: {dbpath}\n")
            self.handle_event('status', 'resume error: no DB')
            return

        try:
            conn = sqlite3.connect(dbpath)
            _ensure_metadata_table(conn)
            last_op = get_metadata(conn, 'last_operation', default=None)
            op_state = get_metadata(conn, 'operation_state', default=None)
            conn.close()
        except Exception as e:
            self._append_output(f"Error reading metadata: {e}\n")
            self.handle_event('status', 'resume error')
            return

        # resume based on last_operation
        if last_op == 'generate_sql':
            self._append_output('\n--- RESUMING GENERATE SQL ---\n')
            self.handle_event('status', 'Resuming generate_sql')
            if not hasattr(self, 'cancel_event'):
                self.cancel_event = threading.Event()
            self.cancel_event.clear()
            startdir = self.entry_dir.get()
            t = threading.Thread(target=self.thread_runner, args=(generate_sql, (startdir, dbpath)), kwargs={'resume': True, 'cancel_event': self.cancel_event}, daemon=True)
            t.start()
            return

        if last_op == 'calculate_duplicates':
            self.resume_calculate_duplicates()
            return

        if last_op == 'delete_duplicates':
            self.resume_delete_duplicates()
            return

        # if nothing to resume
        self._append_output('No resumable operation recorded in DB.\n')
        self.handle_event('status', 'resume not available')

    def resume_filegroup(self):
        """Resume FileGrouper operation from metadata state."""
        origin_dir = self.filegroupstart_dir.get().strip()
        dest_dir   = self.filegroupdest_dir.get().strip()
        db_path    = self.entry_dbname.get().strip()

        grouping_mode = self.filegroupactionselector.get().strip()
        copy_duplicates = self.copyduplicatesselector.get().strip()

        copy_dups = ("copy" in copy_duplicates.lower())

        self.cancel_event.clear()
        self.status_data_label.config(text="FileGrouper: Resuming...")
        self.status_data_label2.config(text="FileGrouper: Resuming...")
        self.status_data_label3.config(text="FileGrouper: Resuming...")
        

        t = threading.Thread(
            target=self.thread_runner,
            args=(group_files_generator, (origin_dir, dest_dir, db_path, grouping_mode, copy_dups)),
            kwargs={'resume': True, 'cancel_event': self.cancel_event, 'dry_run': False},
            daemon=True
        )
        t.start()



# -------------------------
# Launcher stub
# -------------------------
# Example for direct run (optional):
if __name__ == '__main__':
     root = tk.Tk()
     app = App(root)
     root.mainloop()

