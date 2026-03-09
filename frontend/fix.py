import os
import re

components_dir = r"c:\abbu\frontend\src\components"
files = []
for root, _, fnames in os.walk(components_dir):
    for fname in fnames:
        if fname.endswith('.tsx') or fname.endswith('.ts'):
            files.append(os.path.join(root, fname))

modified = 0
for f in files:
    with open(f, 'r', encoding='utf-8') as file:
        content = file.read()
    
    if 'fetch(' in content and 'API_BASE' in content:
        # Check if fetchAuth is already there
        if 'fetchAuth' not in content:
            # find depth
            depth = f.count(os.sep) - components_dir.count(os.sep)
            if depth == 1:
                rel = '../../api/client'
            elif depth == 2:
                rel = '../../../api/client'
            else:
                rel = '../../api/client'
            
            # Remove old API_BASE 
            content = re.sub(r"const API_BASE = import\.meta\.env\.VITE_API_BASE \|\| ['\"]http://localhost:8000['\"];\n+", "", content)
            
            # Insert import at top (after imports)
            last_import = content.rfind("import ")
            if last_import != -1:
                end_of_line = content.find("\n", last_import) + 1
                content = content[:end_of_line] + f"import {{ fetchAuth, API_BASE }} from '{rel}';\n" + content[end_of_line:]
            
            # replace fetch with fetchAuth
            content = content.replace("fetch(", "fetchAuth(")
            
            with open(f, 'w', encoding='utf-8') as file:
                file.write(content)
            modified += 1
            print(f"Updated {f}")

print(f"Total files updated: {modified}")
