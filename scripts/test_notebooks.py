import json
import sys

def test_notebook(nb_path):
    print(f"Testing {nb_path}...")
    with open(nb_path, "r", encoding="utf-8") as f:
        nb = json.load(f)
    
    code_cells = [c for c in nb.get("cells", []) if c.get("cell_type") == "code"]
    print(f"  Found {len(code_cells)} code cells.")
    
    env = {}
    for idx, cell in enumerate(code_cells):
        code = "".join(cell.get("source", []))
        try:
            compiled = compile(code, f"{nb_path}_cell_{idx}", "exec")
            exec(compiled, env)
            print(f"  [Cell {idx+1}/{len(code_cells)}] OK")
        except Exception as e:
            print(f"  [Cell {idx+1}/{len(code_cells)}] FAILED: {type(e).__name__}: {e}")
            return False
    return True

if __name__ == "__main__":
    p_ok = test_notebook("Producer.ipynb")
    c_ok = test_notebook("Consumer.ipynb")
    if p_ok and c_ok:
        print("\nAll notebooks passed successfully!")
        sys.exit(0)
    else:
        print("\nNotebook test failed!")
        sys.exit(1)
