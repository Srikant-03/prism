import os
import sys

EXCLUDE_PARTS = {
    'node_modules', 'venv', '.venv', '__pycache__', 
    '.pytest_cache', '.ruff_cache', '.git', '.zenflow', 
    '.zencoder', 'dist', 'build', '.prism_cache'
}

EXCLUDE_EXACT = {
    'profiling_debug.log', 'test_out.log', 'package-lock.json'
}

EXCLUDE_DIRS = {
    'backend/uploads', 'uploads', 'temp', 'audit_report'
}

def is_excluded(path_str):
    norm = path_str.replace('\\', '/')
    parts = norm.split('/')
    for p in parts:
        if p in EXCLUDE_PARTS:
            return True
    if any(norm == d or norm.startswith(d + '/') for d in EXCLUDE_DIRS):
        return True
    if os.path.basename(norm) in EXCLUDE_EXACT:
        return True
    return False

def get_all_files():
    files = []
    for root, dirs, filenames in os.walk('.'):
        norm_root = root.replace('\\', '/')
        if is_excluded(norm_root):
            dirs[:] = []
            continue
        # filter subdirs in place
        dirs[:] = [d for d in dirs if not is_excluded(os.path.join(root, d))]
        for f in filenames:
            rel = os.path.normpath(os.path.join(root, f)).replace('\\', '/')
            if not is_excluded(rel):
                files.append(rel)
    return sorted(files)

if __name__ == '__main__':
    all_files = get_all_files()
    print(f"Total non-ignored files: {len(all_files)}")
    by_ext = {}
    for f in all_files:
        ext = os.path.splitext(f)[1]
        by_ext[ext] = by_ext.get(ext, 0) + 1
    print("Files by extension:")
    for ext, count in sorted(by_ext.items(), key=lambda x: -x[1]):
        print(f"  {ext or '(no ext)'}: {count}")
    
    print("\nFiles by top directory:")
    by_top = {}
    for f in all_files:
        top = f.split('/')[0]
        by_top[top] = by_top.get(top, 0) + 1
    for top, count in sorted(by_top.items(), key=lambda x: -x[1]):
        print(f"  {top}: {count}")
